//! SDK Session ID Redis Operations
//!
//! This module handles Redis operations for SDK session ID management.
//! Session IDs are used to validate SDK authorization and ensure that
//! only the most recent session is valid for a payment intent.

use common_utils::{
    errors::CustomResult,
    generate_id,
    id_type::{MerchantId, PaymentId},
};
use error_stack::ResultExt;
use redis_interface::DelReply;
use router_env::{instrument, logger, tracing};
use time::PrimitiveDateTime;

use crate::{db::errors, SessionState};

/// Redis key prefix for SDK session storage
const SDK_SESSION_KEY_PREFIX: &str = "sdk_session";

/// Manager for SDK session ID Redis operations
pub struct SdkSessionRedisManager;

impl SdkSessionRedisManager {
    /// Generate Redis key in format: sdk_session:{merchant_id}:{payment_id}
    fn get_session_key(merchant_id: &MerchantId, payment_id: &PaymentId) -> String {
        format!(
            "{}:{}:{}",
            SDK_SESSION_KEY_PREFIX,
            merchant_id.get_string_repr(),
            payment_id.get_string_repr()
        )
    }

    /// Create a new session ID and store in Redis with TTL matching payment intent expiry
    ///
    /// # Arguments
    /// * `state` - Application state with Redis connection
    /// * `merchant_id` - Merchant ID for the payment
    /// * `payment_id` - Payment ID for the session
    /// * `session_expiry` - Expiry time for the session (matches payment intent expiry)
    ///
    /// # Returns
    /// The generated session ID string
    #[instrument(skip_all)]
    pub async fn create_session(
        state: &SessionState,
        merchant_id: &MerchantId,
        payment_id: &PaymentId,
        session_expiry: PrimitiveDateTime,
    ) -> CustomResult<String, errors::StorageError> {
        let redis_conn =
            state
                .store
                .get_redis_conn()
                .change_context(errors::StorageError::RedisError(
                    errors::RedisError::RedisConnectionError.into(),
                ))?;

        // Generate a unique session ID (32 characters)
        let session_id = generate_id(32, "");

        let key = Self::get_session_key(merchant_id, payment_id);

        // Calculate TTL in seconds from now until session_expiry
        let now = common_utils::date_time::now();
        let ttl_seconds = (session_expiry - now).as_seconds_f64() as i64;

        if ttl_seconds <= 0 {
            return Err(errors::StorageError::ValueNotFound(
                "Session expiry is in the past".to_string(),
            ))
            .into_report();
        }

        // Store session ID with TTL
        redis_conn
            .set_key_with_expiry(&key.into(), &session_id, ttl_seconds)
            .await
            .change_context(errors::StorageError::RedisError(
                errors::RedisError::SetHashFieldFailed.into(),
            ))?;

        logger::debug!(
            merchant_id = %merchant_id.get_string_repr(),
            payment_id = %payment_id.get_string_repr(),
            ttl_seconds,
            "Created SDK session ID with TTL"
        );

        Ok(session_id)
    }

    /// Invalidate (delete) existing session for a payment
    ///
    /// # Arguments
    /// * `state` - Application state with Redis connection
    /// * `merchant_id` - Merchant ID for the payment
    /// * `payment_id` - Payment ID for the session
    ///
    /// # Returns
    /// `true` if a session was deleted, `false` if no session existed
    #[instrument(skip_all)]
    pub async fn invalidate_session(
        state: &SessionState,
        merchant_id: &MerchantId,
        payment_id: &PaymentId,
    ) -> CustomResult<bool, errors::StorageError> {
        let redis_conn =
            state
                .store
                .get_redis_conn()
                .change_context(errors::StorageError::RedisError(
                    errors::RedisError::RedisConnectionError.into(),
                ))?;

        let key = Self::get_session_key(merchant_id, payment_id);

        match redis_conn.delete_key(&key.into()).await {
            Ok(DelReply::KeyDeleted) => {
                logger::debug!(
                    merchant_id = %merchant_id.get_string_repr(),
                    payment_id = %payment_id.get_string_repr(),
                    "Invalidated SDK session"
                );
                Ok(true)
            }
            Ok(DelReply::KeyNotDeleted) => {
                logger::debug!(
                    merchant_id = %merchant_id.get_string_repr(),
                    payment_id = %payment_id.get_string_repr(),
                    "No existing SDK session to invalidate"
                );
                Ok(false)
            }
            Err(err) => {
                logger::error!(?err, "Failed to delete session key");
                Ok(false)
            }
        }
    }

    /// Validate session ID against Redis
    ///
    /// # Arguments
    /// * `state` - Application state with Redis connection
    /// * `merchant_id` - Merchant ID for the payment
    /// * `payment_id` - Payment ID for the session
    /// * `session_id` - Session ID to validate
    ///
    /// # Returns
    /// `true` if the session is valid (exists and matches), `false` otherwise
    #[instrument(skip_all)]
    pub async fn validate_session(
        state: &SessionState,
        merchant_id: &MerchantId,
        payment_id: &PaymentId,
        session_id: &str,
    ) -> CustomResult<bool, errors::StorageError> {
        let redis_conn =
            state
                .store
                .get_redis_conn()
                .change_context(errors::StorageError::RedisError(
                    errors::RedisError::RedisConnectionError.into(),
                ))?;

        let key = Self::get_session_key(merchant_id, payment_id);

        let stored_session_id: String = redis_conn.get_key(&key.into()).await.change_context(
            errors::StorageError::ValueNotFound("Session not found or expired".to_string()),
        )?;

        let is_valid = stored_session_id == session_id;

        logger::debug!(
            merchant_id = %merchant_id.get_string_repr(),
            payment_id = %payment_id.get_string_repr(),
            is_valid,
            "Validated SDK session ID"
        );

        Ok(is_valid)
    }

    /// Get the current session ID for a payment without validating
    ///
    /// # Arguments
    /// * `state` - Application state with Redis connection
    /// * `merchant_id` - Merchant ID for the payment
    /// * `payment_id` - Payment ID for the session
    ///
    /// # Returns
    /// The current session ID if it exists
    #[instrument(skip_all)]
    pub async fn get_session(
        state: &SessionState,
        merchant_id: &MerchantId,
        payment_id: &PaymentId,
    ) -> CustomResult<Option<String>, errors::StorageError> {
        let redis_conn =
            state
                .store
                .get_redis_conn()
                .change_context(errors::StorageError::RedisError(
                    errors::RedisError::RedisConnectionError.into(),
                ))?;

        let key = Self::get_session_key(merchant_id, payment_id);

        match redis_conn.get_key::<String>(&key.into()).await {
            Ok(session_id) => {
                logger::debug!(
                    merchant_id = %merchant_id.get_string_repr(),
                    payment_id = %payment_id.get_string_repr(),
                    "Retrieved SDK session ID"
                );
                Ok(Some(session_id))
            }
            Err(_) => {
                logger::debug!(
                    merchant_id = %merchant_id.get_string_repr(),
                    payment_id = %payment_id.get_string_repr(),
                    "No SDK session ID found"
                );
                Ok(None)
            }
        }
    }
}
