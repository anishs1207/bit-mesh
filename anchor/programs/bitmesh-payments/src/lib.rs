#![allow(unexpected_cfgs)]

use anchor_lang::prelude::*;
use anchor_lang::system_program::{transfer, Transfer};

declare_id!("Fg6PaFpoGXkYsidMpWxTWqkZkx2J6G8sT7BLCFQyK6G");

const MAX_LEASE_SECONDS: i64 = 365 * 24 * 60 * 60;

#[program]
pub mod bitmesh_payments {
    use super::*;

    pub fn create_lease(
        ctx: Context<CreateLease>,
        manifest: [u8; 32],
        expires_at: i64,
        amount: u64,
    ) -> Result<()> {
        let now = Clock::get()?.unix_timestamp;
        require!(amount > 0, PaymentError::ZeroAmount);
        require!(manifest != [0; 32], PaymentError::InvalidManifest);
        require!(expires_at > now, PaymentError::InvalidExpiry);
        require!(
            expires_at <= now.saturating_add(MAX_LEASE_SECONDS),
            PaymentError::LeaseTooLong
        );
        require_keys_neq!(
            ctx.accounts.payer.key(),
            ctx.accounts.provider.key(),
            PaymentError::SelfPayment
        );

        transfer(
            CpiContext::new(
                ctx.accounts.system_program.to_account_info(),
                Transfer {
                    from: ctx.accounts.payer.to_account_info(),
                    to: ctx.accounts.lease.to_account_info(),
                },
            ),
            amount,
        )?;

        let lease = &mut ctx.accounts.lease;
        lease.payer = ctx.accounts.payer.key();
        lease.provider = ctx.accounts.provider.key();
        lease.manifest = manifest;
        lease.amount = amount;
        lease.expires_at = expires_at;
        lease.bump = ctx.bumps.lease;

        emit!(LeaseCreated {
            lease: lease.key(),
            payer: lease.payer,
            provider: lease.provider,
            manifest,
            amount,
            expires_at,
        });
        Ok(())
    }

    pub fn settle(ctx: Context<Settle>) -> Result<()> {
        let lease = &ctx.accounts.lease;
        require!(
            Clock::get()?.unix_timestamp >= lease.expires_at,
            PaymentError::LeaseActive
        );
        require!(
            lease.to_account_info().lamports() >= lease.amount,
            PaymentError::EscrowUnderfunded
        );

        **lease.to_account_info().try_borrow_mut_lamports()? -= lease.amount;
        **ctx
            .accounts
            .provider
            .to_account_info()
            .try_borrow_mut_lamports()? += lease.amount;

        emit!(LeaseSettled {
            lease: lease.key(),
            provider: lease.provider,
            amount: lease.amount,
        });
        // Anchor closes the lease and returns its rent to the payer after this instruction.
        Ok(())
    }
}

#[derive(Accounts)]
#[instruction(manifest: [u8; 32])]
pub struct CreateLease<'info> {
    #[account(mut)]
    pub payer: Signer<'info>,
    /// CHECK: Stored in the PDA and required to sign settlement.
    pub provider: UncheckedAccount<'info>,
    #[account(
        init,
        payer = payer,
        space = Lease::SPACE,
        seeds = [b"lease", payer.key().as_ref(), provider.key().as_ref(), manifest.as_ref()],
        bump
    )]
    pub lease: Account<'info, Lease>,
    pub system_program: Program<'info, System>,
}

#[derive(Accounts)]
pub struct Settle<'info> {
    #[account(mut, has_one = payer, has_one = provider, close = payer)]
    pub lease: Account<'info, Lease>,
    /// CHECK: Constrained to the stored payer and receives refunded rent.
    #[account(mut)]
    pub payer: UncheckedAccount<'info>,
    #[account(mut)]
    pub provider: Signer<'info>,
}

#[account]
pub struct Lease {
    pub payer: Pubkey,
    pub provider: Pubkey,
    pub manifest: [u8; 32],
    pub amount: u64,
    pub expires_at: i64,
    pub bump: u8,
}

impl Lease {
    pub const SPACE: usize = 8 + 32 + 32 + 32 + 8 + 8 + 1;
}

#[event]
pub struct LeaseCreated {
    pub lease: Pubkey,
    pub payer: Pubkey,
    pub provider: Pubkey,
    pub manifest: [u8; 32],
    pub amount: u64,
    pub expires_at: i64,
}

#[event]
pub struct LeaseSettled {
    pub lease: Pubkey,
    pub provider: Pubkey,
    pub amount: u64,
}

#[error_code]
pub enum PaymentError {
    #[msg("payment must be non-zero")]
    ZeroAmount,
    #[msg("manifest digest cannot be all zeroes")]
    InvalidManifest,
    #[msg("lease expiry must be in the future")]
    InvalidExpiry,
    #[msg("lease duration exceeds one year")]
    LeaseTooLong,
    #[msg("payer and provider must be different accounts")]
    SelfPayment,
    #[msg("lease has not expired")]
    LeaseActive,
    #[msg("lease escrow does not contain the agreed payment")]
    EscrowUnderfunded,
}
