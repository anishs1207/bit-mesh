use anchor_lang::prelude::*;
use anchor_lang::system_program::{transfer, Transfer};

declare_id!("11111111111111111111111111111111");

#[program]
pub mod bitmesh_payments {
    use super::*;
    pub fn create_lease(ctx: Context<CreateLease>, manifest: [u8; 32], expires_at: i64, amount: u64) -> Result<()> {
        require!(amount > 0, PaymentError::ZeroAmount);
        require!(expires_at > Clock::get()?.unix_timestamp, PaymentError::InvalidExpiry);
        transfer(CpiContext::new(ctx.accounts.system_program.to_account_info(), Transfer { from: ctx.accounts.payer.to_account_info(), to: ctx.accounts.lease.to_account_info() }), amount)?;
        let lease = &mut ctx.accounts.lease; lease.payer = ctx.accounts.payer.key(); lease.provider = ctx.accounts.provider.key(); lease.manifest = manifest; lease.amount = amount; lease.expires_at = expires_at; lease.bump = ctx.bumps.lease; lease.settled = false; Ok(())
    }
    pub fn settle(ctx: Context<Settle>) -> Result<()> {
        let lease = &mut ctx.accounts.lease; require!(!lease.settled, PaymentError::AlreadySettled); require!(Clock::get()?.unix_timestamp >= lease.expires_at, PaymentError::LeaseActive);
        let amount = lease.to_account_info().lamports().saturating_sub(Rent::get()?.minimum_balance(Lease::SPACE));
        **lease.to_account_info().try_borrow_mut_lamports()? -= amount; **ctx.accounts.provider.try_borrow_mut_lamports()? += amount; lease.settled = true; Ok(())
    }
}

#[derive(Accounts)] #[instruction(manifest: [u8; 32])]
pub struct CreateLease<'info> {
    #[account(mut)] pub payer: Signer<'info>,
    /// CHECK: Payment destination recorded in the lease and validated on settlement.
    pub provider: UncheckedAccount<'info>,
    #[account(init, payer = payer, space = Lease::SPACE, seeds = [b"lease", payer.key().as_ref(), provider.key().as_ref(), manifest.as_ref()], bump)] pub lease: Account<'info, Lease>,
    pub system_program: Program<'info, System>,
}
#[derive(Accounts)]
pub struct Settle<'info> {
    #[account(mut, has_one = provider)] pub lease: Account<'info, Lease>,
    /// CHECK: constrained to the provider stored in the lease.
    #[account(mut)] pub provider: UncheckedAccount<'info>,
}
#[account] pub struct Lease { pub payer: Pubkey, pub provider: Pubkey, pub manifest: [u8; 32], pub amount: u64, pub expires_at: i64, pub bump: u8, pub settled: bool }
impl Lease { pub const SPACE: usize = 8 + 32 + 32 + 32 + 8 + 8 + 1 + 1; }
#[error_code] pub enum PaymentError { #[msg("payment must be non-zero")] ZeroAmount, #[msg("lease expiry must be in the future")] InvalidExpiry, #[msg("lease has not expired")] LeaseActive, #[msg("lease has already been settled")] AlreadySettled }
