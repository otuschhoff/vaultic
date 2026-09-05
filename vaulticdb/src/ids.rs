//! Strongly typed identifiers shared by storage, encryption, and broker protocols.

use std::{borrow::Borrow, fmt, ops::Deref};

use serde::{Deserialize, Serialize};

macro_rules! string_newtype {
    ($name:ident) => {
        #[derive(
            Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_string(self) -> String {
                self.0
            }

            pub fn is_empty(&self) -> bool {
                self.0.is_empty()
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl Borrow<str> for $name {
            fn borrow(&self) -> &str {
                self.as_str()
            }
        }

        impl Deref for $name {
            type Target = str;

            fn deref(&self) -> &Self::Target {
                self.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }

        impl From<&$name> for $name {
            fn from(value: &$name) -> Self {
                value.clone()
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl PartialEq<str> for $name {
            fn eq(&self, other: &str) -> bool {
                self.as_str() == other
            }
        }

        impl PartialEq<&str> for $name {
            fn eq(&self, other: &&str) -> bool {
                self.as_str() == *other
            }
        }
    };
}

string_newtype!(RepositoryId);
string_newtype!(SessionId);
string_newtype!(MemberId);
string_newtype!(Namespace);

#[cfg(test)]
mod tests {
    //! Identifier conversion and wire-compatibility tests.

    use super::*;

    #[test]
    fn newtypes_preserve_string_json_representation() {
        assert_eq!(
            serde_json::to_string(&RepositoryId::from("repo-a")).unwrap(),
            "\"repo-a\""
        );
        assert_eq!(
            serde_json::to_string(&SessionId::from("session-a")).unwrap(),
            "\"session-a\""
        );
        assert_eq!(
            serde_json::to_string(&MemberId::from("member-a")).unwrap(),
            "\"member-a\""
        );
        assert_eq!(
            serde_json::to_string(&Namespace::from("default")).unwrap(),
            "\"default\""
        );
    }

    #[test]
    fn newtypes_deserialize_from_existing_string_wire_values() {
        let repository_id: RepositoryId = serde_json::from_str("\"repo-a\"").unwrap();
        let session_id: SessionId = serde_json::from_str("\"session-a\"").unwrap();
        let member_id: MemberId = serde_json::from_str("\"member-a\"").unwrap();
        let namespace: Namespace = serde_json::from_str("\"default\"").unwrap();

        assert_eq!(repository_id, "repo-a");
        assert_eq!(session_id, "session-a");
        assert_eq!(member_id, "member-a");
        assert_eq!(namespace, "default");
    }
}
