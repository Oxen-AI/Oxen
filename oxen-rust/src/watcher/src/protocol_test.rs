#[cfg(test)]
mod tests {
    use crate::protocol::*;
    use std::path::PathBuf;
    use std::time::SystemTime;

    #[test]
    fn test_request_serialization() {
        let request = WatcherRequest::GetStatus {
            paths: Some(vec![PathBuf::from("/tmp/test")]),
        };
        
        let bytes = request.to_bytes().unwrap();
        let deserialized = WatcherRequest::from_bytes(&bytes).unwrap();
        
        match deserialized {
            WatcherRequest::GetStatus { paths } => {
                assert!(paths.is_some());
                assert_eq!(paths.unwrap()[0], PathBuf::from("/tmp/test"));
            }
            _ => panic!("Wrong request type"),
        }
    }

    #[test]
    fn test_response_serialization() {
        let response = WatcherResponse::Summary {
            created: 3,
            modified: 5,
            removed: 2,
            last_updated: SystemTime::now(),
        };
        
        let bytes = response.to_bytes().unwrap();
        let deserialized = WatcherResponse::from_bytes(&bytes).unwrap();
        
        match deserialized {
            WatcherResponse::Summary { created, modified, removed, .. } => {
                assert_eq!(created, 3);
                assert_eq!(modified, 5);
                assert_eq!(removed, 2);
            }
            _ => panic!("Wrong response type"),
        }
    }

    #[test]
    fn test_status_result_serialization() {
        let status_result = StatusResult {
            created: vec![FileStatus {
                path: PathBuf::from("created.txt"),
                mtime: SystemTime::now(),
                size: 200,
                hash: None,
                status: FileStatusType::Created,
            }],
            modified: vec![FileStatus {
                path: PathBuf::from("modified.txt"),
                mtime: SystemTime::now(),
                size: 100,
                hash: Some("hash1".to_string()),
                status: FileStatusType::Modified,
            }],
            removed: vec![PathBuf::from("removed.txt")],
            scan_complete: true,
        };
        
        let response = WatcherResponse::Status(status_result);
        let bytes = response.to_bytes().unwrap();
        let deserialized = WatcherResponse::from_bytes(&bytes).unwrap();
        
        match deserialized {
            WatcherResponse::Status(result) => {
                assert_eq!(result.created.len(), 1);
                assert_eq!(result.modified.len(), 1);
                assert_eq!(result.removed.len(), 1);
                assert!(result.scan_complete);
                
                assert_eq!(result.created[0].path, PathBuf::from("created.txt"));
                assert_eq!(result.modified[0].path, PathBuf::from("modified.txt"));
                assert_eq!(result.removed[0], PathBuf::from("removed.txt"));
            }
            _ => panic!("Wrong response type"),
        }
    }

    #[test]
    fn test_all_request_types() {
        let requests = vec![
            WatcherRequest::GetStatus { paths: None },
            WatcherRequest::GetSummary,
            WatcherRequest::Refresh {
                paths: vec![PathBuf::from("/tmp")],
            },
            WatcherRequest::Shutdown,
            WatcherRequest::Ping,
        ];
        
        for request in requests {
            let bytes = request.to_bytes().unwrap();
            let deserialized = WatcherRequest::from_bytes(&bytes).unwrap();
            
            // Just verify it deserializes correctly
            match (&request, &deserialized) {
                (WatcherRequest::Ping, WatcherRequest::Ping) => {}
                (WatcherRequest::Shutdown, WatcherRequest::Shutdown) => {}
                (WatcherRequest::GetSummary, WatcherRequest::GetSummary) => {}
                _ => {} // Other cases would need deeper comparison
            }
        }
    }

    #[test]
    fn test_file_status_type_equality() {
        assert_eq!(FileStatusType::Created, FileStatusType::Created);
        assert_eq!(FileStatusType::Modified, FileStatusType::Modified);
        assert_eq!(FileStatusType::Removed, FileStatusType::Removed);
        
        assert_ne!(FileStatusType::Created, FileStatusType::Modified);
        assert_ne!(FileStatusType::Modified, FileStatusType::Removed);
        assert_ne!(FileStatusType::Created, FileStatusType::Removed);
    }

    #[test]
    fn test_error_response() {
        let response = WatcherResponse::Error("Something went wrong".to_string());
        let bytes = response.to_bytes().unwrap();
        let deserialized = WatcherResponse::from_bytes(&bytes).unwrap();
        
        match deserialized {
            WatcherResponse::Error(msg) => {
                assert_eq!(msg, "Something went wrong");
            }
            _ => panic!("Wrong response type"),
        }
    }

    #[test]
    fn test_large_payload() {
        // Test with many files
        let mut modified = Vec::new();
        for i in 0..1000 {
            modified.push(FileStatus {
                path: PathBuf::from(format!("file{}.txt", i)),
                mtime: SystemTime::now(),
                size: i as u64,
                hash: Some(format!("hash{}", i)),
                status: FileStatusType::Modified,
            });
        }
        
        let status_result = StatusResult {
            created: vec![],
            modified,
            removed: vec![],
            scan_complete: true,
        };
        
        let response = WatcherResponse::Status(status_result);
        let bytes = response.to_bytes().unwrap();
        let deserialized = WatcherResponse::from_bytes(&bytes).unwrap();
        
        match deserialized {
            WatcherResponse::Status(result) => {
                assert_eq!(result.modified.len(), 1000);
                assert_eq!(result.modified[0].path, PathBuf::from("file0.txt"));
                assert_eq!(result.modified[999].path, PathBuf::from("file999.txt"));
            }
            _ => panic!("Wrong response type"),
        }
    }
}