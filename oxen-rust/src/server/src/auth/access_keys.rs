use liboxen::error::OxenError;
use liboxen::util;
use liboxen::model::{NewUser, User};

use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use rand_core::OsRng;
use rocksdb::{DBWithThreadMode, LogLevel, MultiThreaded, Options};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::str;
use x25519_dalek::{EphemeralSecret, PublicKey};

pub const SECRET_KEY_FILENAME: &str = "SECRET_KEY_BASE";

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct JWTClaim {
    id: String,
    name: String,
    email: String,
}

pub struct KeyGenerator {
    sync_dir: PathBuf,
    db: DBWithThreadMode<MultiThreaded>,
}

impl KeyGenerator {
    fn secret_key_path(sync_dir: &Path) -> PathBuf {
        let hidden_dir = util::fs::oxen_hidden_dir(sync_dir);
        hidden_dir.join(SECRET_KEY_FILENAME)
    }

    pub fn new(sync_dir: &Path) -> Result<KeyGenerator, OxenError> {
        let hidden_dir = util::fs::oxen_hidden_dir(sync_dir);
        if !hidden_dir.exists() {
            std::fs::create_dir_all(&hidden_dir)?;
        }

        let db_dir = hidden_dir.join("keys");
        let mut opts = Options::default();
        opts.set_log_level(LogLevel::Error);
        opts.create_if_missing(true);

        let secret_file = KeyGenerator::secret_key_path(sync_dir);
        if !secret_file.exists() {
            // Not really using this in the right context...but fine to generate random hash for now
            let secret = EphemeralSecret::new(OsRng);
            let public = PublicKey::from(&secret);
            let key = hex::encode(public.as_bytes());
            log::debug!("Got secret key: {}", key);
            util::fs::write_to_path(&secret_file, &key);
        }

        Ok(KeyGenerator {
            sync_dir: sync_dir.to_path_buf(),
            db: DBWithThreadMode::open(&opts, &db_dir)?,
        })
    }

    pub fn create(&self, user: &NewUser) -> Result<User, OxenError> {
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
                Ok(User {
                    id: user_claims.id.to_owned(),
                    name: user_claims.name.to_owned(),
                    email: user_claims.email.to_owned(),
                    token: token.to_owned(),
                })
            }
            Err(_) => {
                let err = format!("Could not create access key for: {:?}", user_claims);
                Err(OxenError::basic_str(&err))
            }
        }
    }

    pub fn get_claim(&self, token: &str) -> Result<Option<JWTClaim>, OxenError> {
        let key = token.as_bytes();
        match self.db.get(key) {
            Ok(Some(value)) => {
                let value = str::from_utf8(&*value)?;
                let decoded_claim = serde_json::from_str(value)?;
                Ok(Some(decoded_claim))
            }
            Ok(None) => Ok(None),
            Err(err) => {
                let err = format!("Err could not red from commit db: {}", err);
                Err(OxenError::basic_str(&err))
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
        let path = KeyGenerator::secret_key_path(&self.sync_dir);
        util::fs::read_from_path(&path)
    }
}

#[cfg(test)]
mod tests {

    use crate::auth::access_keys::KeyGenerator;
    use crate::test;
    use liboxen::error::OxenError;
    use liboxen::model::NewUser;

    #[test]
    fn test_constructor() -> Result<(), OxenError> {
        test::run_empty_sync_dir_test(|sync_dir| {
            let keygen_result = KeyGenerator::new(sync_dir);
            assert!(keygen_result.is_ok());
            Ok(())
        })
    }

    #[test]
    fn test_generate_key() -> Result<(), OxenError> {
        test::run_empty_sync_dir_test(|sync_dir| {
            let keygen = KeyGenerator::new(sync_dir)?;
            let new_user = NewUser {
                name: String::from("Ox"),
                email: String::from("ox@oxen.ai"),
            };
            let user = keygen.create(&new_user)?;
            assert!(!user.token.is_empty());

            Ok(())
        })
    }

    #[test]
    fn test_generate_and_get_key() -> Result<(), OxenError> {
        test::run_empty_sync_dir_test(|sync_dir| {
            let keygen = KeyGenerator::new(sync_dir)?;
            let new_user = NewUser {
                name: String::from("Ox"),
                email: String::from("ox@oxen.ai"),
            };
            let user = keygen.create(&new_user)?;
            let fetched_claim = keygen.get_claim(&user.token)?;
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
            let keygen = KeyGenerator::new(sync_dir)?;
            let new_user = NewUser {
                name: String::from("Ox"),
                email: String::from("ox@oxen.ai"),
            };
            let user = keygen.create(&new_user)?;
            let is_valid = keygen.token_is_valid(&user.token);
            assert!(is_valid);
            Ok(())
        })
    }

    #[test]
    fn test_invalid_key() -> Result<(), OxenError> {
        test::run_empty_sync_dir_test(|sync_dir| {
            let keygen = KeyGenerator::new(sync_dir)?;

            let is_valid = keygen.token_is_valid("not-a-valid-key");
            assert!(!is_valid);

            Ok(())
        })
    }
}
