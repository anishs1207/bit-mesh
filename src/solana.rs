use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Duration, timeout},
};

pub const PROGRAM_ID: &str = "Fg6PaFpoGXkYsidMpWxTWqkZkx2J6G8sT7BLCFQyK6G";

pub struct PaymentClaim<'a> {
    pub account: &'a str,
    pub payer: &'a str,
    pub provider: &'a str,
    pub manifest: &'a str,
    pub expires_at: u64,
}

pub async fn verify_lease(rpc: &str, claim: &PaymentClaim<'_>) -> Result<()> {
    let account = rpc_account(rpc, claim.account).await?;
    let value = account
        .get("result")
        .and_then(|r| r.get("value"))
        .filter(|v| !v.is_null())
        .context("payment lease account not found")?;
    if value.get("owner").and_then(Value::as_str) != Some(PROGRAM_ID) {
        bail!("payment lease is not owned by the BitMesh Anchor program");
    }
    let encoded = value
        .get("data")
        .and_then(Value::as_array)
        .and_then(|v| v.first())
        .and_then(Value::as_str)
        .context("payment lease has no base64 data")?;
    let bytes = decode_base64(encoded)?;
    if bytes.len() != 121 {
        bail!("invalid payment lease account size");
    }
    let discriminator = Sha256::digest(b"account:Lease");
    if bytes[..8] != discriminator[..8] {
        bail!("invalid payment lease discriminator");
    }
    if bytes[8..40] != decode_base58_32(claim.payer)?
        || bytes[40..72] != decode_base58_32(claim.provider)?
        || &bytes[72..104] != hex::decode(claim.manifest)?.as_slice()
    {
        bail!("payment lease payer, provider, or manifest mismatch");
    }
    let amount = u64::from_le_bytes(bytes[104..112].try_into()?);
    let expiry = i64::from_le_bytes(bytes[112..120].try_into()?);
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    if amount == 0 || expiry < 0 || (expiry as u64) < claim.expires_at || (expiry as u64) <= now {
        bail!("payment lease has no amount or insufficient remaining duration");
    }
    if value.get("lamports").and_then(Value::as_u64).unwrap_or(0) < amount {
        bail!("payment lease escrow is underfunded");
    }
    Ok(())
}

async fn rpc_account(rpc: &str, account: &str) -> Result<Value> {
    let endpoint = rpc
        .strip_prefix("http://")
        .context("Solana RPC must use http://; put a TLS proxy in front for remote use")?;
    let (authority, path) = endpoint.split_once('/').unwrap_or((endpoint, ""));
    let address = if authority.contains(':') {
        authority.to_owned()
    } else {
        format!("{authority}:80")
    };
    let body = json!({"jsonrpc":"2.0","id":1,"method":"getAccountInfo",
        "params":[account,{"encoding":"base64","commitment":"finalized"}]})
    .to_string();
    let request = format!(
        "POST /{path} HTTP/1.1\r\nHost: {authority}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let response = timeout(Duration::from_secs(10), async {
        let mut stream = TcpStream::connect(address).await?;
        stream.write_all(request.as_bytes()).await?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        Ok::<_, anyhow::Error>(response)
    })
    .await
    .context("Solana RPC timed out")??;
    let split = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .context("invalid Solana RPC HTTP response")?;
    let headers = std::str::from_utf8(&response[..split])?;
    if !headers.starts_with("HTTP/1.1 200 ") && !headers.starts_with("HTTP/1.0 200 ") {
        bail!("Solana RPC returned a non-200 response");
    }
    if headers
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        bail!("chunked Solana RPC responses require a non-chunked proxy");
    }
    let value: Value = serde_json::from_slice(&response[split + 4..])?;
    if let Some(error) = value.get("error") {
        bail!("Solana RPC error: {error}");
    }
    Ok(value)
}

fn decode_base64(input: &str) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut buffer = 0u32;
    let mut bits = 0u8;
    for byte in input.bytes() {
        if byte == b'=' {
            break;
        }
        let digit = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => bail!("invalid base64 lease data"),
        };
        buffer = (buffer << 6) | u32::from(digit);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            output.push((buffer >> bits) as u8);
            buffer &= (1 << bits) - 1;
        }
    }
    Ok(output)
}

fn decode_base58_32(input: &str) -> Result<[u8; 32]> {
    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    let mut value = [0u8; 32];
    for byte in input.bytes() {
        let digit = ALPHABET
            .iter()
            .position(|candidate| *candidate == byte)
            .context("invalid Solana public key")? as u32;
        let mut carry = digit;
        for octet in value.iter_mut().rev() {
            carry += u32::from(*octet) * 58;
            *octet = carry as u8;
            carry >>= 8;
        }
        if carry != 0 {
            bail!("Solana public key is too long");
        }
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decodes_anchor_keys_and_data() {
        assert_eq!(
            decode_base58_32("11111111111111111111111111111111").unwrap(),
            [0; 32]
        );
        assert_eq!(decode_base64("YWJj").unwrap(), b"abc");
        assert!(decode_base58_32("0").is_err());
    }

    #[tokio::test]
    async fn verifies_finalized_anchor_escrow_fields() {
        let payer = "11111111111111111111111111111111";
        let provider = PROGRAM_ID;
        let manifest = hex::encode([7u8; 32]);
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let mut data = Vec::new();
        data.extend_from_slice(&Sha256::digest(b"account:Lease")[..8]);
        data.extend_from_slice(&decode_base58_32(payer).unwrap());
        data.extend_from_slice(&decode_base58_32(provider).unwrap());
        data.extend_from_slice(&[7u8; 32]);
        data.extend_from_slice(&5u64.to_le_bytes());
        data.extend_from_slice(&(expiry as i64).to_le_bytes());
        data.push(1);
        const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut encoded = String::new();
        for chunk in data.chunks(3) {
            let a = chunk[0];
            let b = *chunk.get(1).unwrap_or(&0);
            let c = *chunk.get(2).unwrap_or(&0);
            encoded.push(TABLE[(a >> 2) as usize] as char);
            encoded.push(TABLE[(((a & 3) << 4) | (b >> 4)) as usize] as char);
            encoded.push(if chunk.len() > 1 {
                TABLE[(((b & 15) << 2) | (c >> 6)) as usize] as char
            } else {
                '='
            });
            encoded.push(if chunk.len() > 2 {
                TABLE[(c & 63) as usize] as char
            } else {
                '='
            });
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            let body = json!({"result":{"value":{"owner":PROGRAM_ID,"lamports":100,
                "data":[encoded,"base64"]}}})
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        verify_lease(
            &format!("http://{address}"),
            &PaymentClaim {
                account: payer,
                payer,
                provider,
                manifest: &manifest,
                expires_at: expiry,
            },
        )
        .await
        .unwrap();
        server.await.unwrap();
    }
}
