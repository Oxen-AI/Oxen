use liboxen::error::OxenError;
use liboxen::model::User;
use liboxen::util;

use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use rocksdb::{DBWithThreadMode, LogLevel, MultiThreaded, Options};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::str;

pub const SECRET_KEY_FILENAME: &str = "SECRET_KEY_BASE";

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct JWTClaim {
    id: String,
    name: String,
    email: String,
}

impl JWTClaim {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn email(&self) -> &str {
        &self.email
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

pub struct AccessKeyManager {
    sync_dir: PathBuf,
    db: DBWithThreadMode<MultiThreaded>,
}

impl AccessKeyManager {
    fn secret_key_path(sync_dir: &Path) -> PathBuf {
        let hidden_dir = util::fs::oxen_hidden_dir(sync_dir);
        hidden_dir.join(SECRET_KEY_FILENAME)
    }

    pub fn new(sync_dir: &Path) -> Result<AccessKeyManager, OxenError> {
        let read_only = false;
        AccessKeyManager::p_new(sync_dir, read_only)
    }

    pub fn new_read_only(sync_dir: &Path) -> Result<AccessKeyManager, OxenError> {
        let read_only = true;
        AccessKeyManager::p_new(sync_dir, read_only)
    }

    fn p_new(sync_dir: &Path, read_only: bool) -> Result<AccessKeyManager, OxenError> {
        let hidden_dir = util::fs::oxen_hidden_dir(sync_dir);
        if !hidden_dir.exists() {
            util::fs::create_dir_all(&hidden_dir)?;
        }

        let db_dir = hidden_dir.join("keys");
        let mut opts = Options::default();
        opts.set_log_level(LogLevel::Fatal);
        opts.create_if_missing(true);

        let secret_file = AccessKeyManager::secret_key_path(sync_dir);
        if !secret_file.exists() {
            // Just generating a random UUID for now
            let secret = uuid::Uuid::new_v4();
            let key = hex::encode(secret.as_bytes());
            log::debug!("Got secret key: {}", key);
            util::fs::write_to_path(&secret_file, &key)?;
        }

        let db = if read_only {
            DBWithThreadMode::open_for_read_only(&opts, dunce::simplified(&db_dir), false)?
        } else {
            DBWithThreadMode::open(&opts, dunce::simplified(&db_dir))?
        };

        Ok(AccessKeyManager {
            sync_dir: sync_dir.to_path_buf(),
            db,
        })
    }

    pub fn create(&self, user: &User) -> Result<(User, String), OxenError> {
        let user_claims = JWTClaim {
            id: format!("{}", uuid::Uuid::new_v4()),
            name: user.name.to_owned(),
            email: user.email.to_owned(),
        };

        let secret_key = self.read_secret_key()?;
        match encode(
            &Header::default(),
            &user_claims,
            &EncodingKey::from_secret(secret_key.as_ref()),
        ) {
            Ok(token) => {
                // We map from token to claims so that
                // when a request comes in with a token we can decode it
                // then check if the claims matches
                // if the token doesn't exist, they def don't have access
                // if they have someone elses token, we can block also (but how likely is this...? maybe sniffing traffic?)
                let encoded_claim = serde_json::to_string(&user_claims)?;
                self.db.put(&token, encoded_claim)?;
                Ok((
                    User {
                        name: user_claims.name.to_owned(),
                        email: user_claims.email.to_owned(),
                    },
                    token,
                ))
            }
            Err(_) => {
                let err = format!("Could not create access key for: {user_claims:?}");
                Err(OxenError::basic_str(err))
            }
        }
    }

    pub fn get_claim(&self, token: &str) -> Result<Option<JWTClaim>, OxenError> {
        let key = token.as_bytes();
        match self.db.get(key) {
            Ok(Some(value)) => {
                let value = str::from_utf8(&value)?;
                let decoded_claim = serde_json::from_str(value)?;
                Ok(Some(decoded_claim))
            }
            Ok(None) => Ok(None),
            Err(err) => {
                let err = format!("Err could not red from commit db: {err}");
                Err(OxenError::basic_str(err))
            }
        }
    }

    pub fn token_is_valid(&self, token: &str) -> bool {
        match self.get_claim(token) {
            Ok(Some(claim)) => {
                let secret = self.read_secret_key();
                if secret.is_err() {
                    return false;
                }

                let mut validator = Validation::new(Algorithm::HS256);
                validator.set_required_spec_claims(&["email"]);
                match decode::<JWTClaim>(
                    token,
                    &DecodingKey::from_secret(secret.unwrap().as_ref()),
                    &validator,
                ) {
                    Ok(token_data) => {
                        // Make sure we decoded the email is the one in our db
                        token_data.claims == claim
                    }
                    _ => {
                        log::info!("auth token is not valid: {}", token);
                        false
                    }
                }
            }
            Ok(None) => false,
            Err(_) => false,
        }
    }

    fn read_secret_key(&self) -> Result<String, OxenError> {
        let path = AccessKeyManager::secret_key_path(&self.sync_dir);
        util::fs::read_from_path(path)
    }
}

#[cfg(test)]
mod tests {

    use crate::auth::access_keys::AccessKeyManager;
    use crate::test;
    use liboxen::error::OxenError;
    use liboxen::model::User;

    #[test]
    fn test_constructor() -> Result<(), OxenError> {
        test::run_empty_sync_dir_test(|sync_dir| {
            let keygen_result = AccessKeyManager::new(sync_dir);
            assert!(keygen_result.is_ok());
            Ok(())
        })
    }

    #[test]
    fn test_generate_key() -> Result<(), OxenError> {
        test::run_empty_sync_dir_test(|sync_dir| {
            let keygen = AccessKeyManager::new(sync_dir)?;
            let new_user = User {
                name: String::from("Ox"),
                email: String::from("ox@oxen.ai"),
            };
            let (_user, token) = keygen.create(&new_user)?;
            assert!(!token.is_empty());

            Ok(())
        })
    }

    #[test]
    fn test_generate_and_get_key() -> Result<(), OxenError> {
        test::run_empty_sync_dir_test(|sync_dir| {
            let keygen = AccessKeyManager::new(sync_dir)?;
            let new_user = User {
                name: String::from("Ox"),
                email: String::from("ox@oxen.ai"),
            };
            let (_user, token) = keygen.create(&new_user)?;
            let fetched_claim = keygen.get_claim(&token)?;
            assert!(fetched_claim.is_some());
            let fetched_claim = fetched_claim.unwrap();
            assert_eq!(new_user.email, fetched_claim.email);
            assert_eq!(new_user.name, fetched_claim.name);

            Ok(())
        })
    }

    #[test]
    fn test_generate_and_validate() -> Result<(), OxenError> {
        test::run_empty_sync_dir_test(|sync_dir| {
            let keygen = AccessKeyManager::new(sync_dir)?;
            let new_user = User {
                name: String::from("Ox"),
                email: String::from("ox@oxen.ai"),
            };
            let (_user, token) = keygen.create(&new_user)?;
            let is_valid = keygen.token_is_valid(&token);
            assert!(is_valid);
            Ok(())
        })
    }

    #[test]
    fn test_invalid_key() -> Result<(), OxenError> {
        test::run_empty_sync_dir_test(|sync_dir| {
            let keygen = AccessKeyManager::new(sync_dir)?;

            let is_valid = keygen.token_is_valid("not-a-valid-key");
            assert!(!is_valid);

            Ok(())
        })
    }
}
