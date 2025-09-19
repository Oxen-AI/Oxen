#[cfg(test)]
mod tests {
    use crate::protocol::*;
    use crate::tree::{FileMetadata, FileSystemTree, TreeNode};
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
    fn test_new_get_tree_request() {
        let request = WatcherRequest::GetTree {
            path: Some(PathBuf::from("src")),
        };
        
        let bytes = request.to_bytes().unwrap();
        let deserialized = WatcherRequest::from_bytes(&bytes).unwrap();
        
        match deserialized {
            WatcherRequest::GetTree { path } => {
                assert!(path.is_some());
                assert_eq!(path.unwrap(), PathBuf::from("src"));
            }
            _ => panic!("Wrong request type"),
        }
    }

    #[test]
    fn test_new_get_metadata_request() {
        let request = WatcherRequest::GetMetadata {
            paths: vec![PathBuf::from("file1.txt"), PathBuf::from("file2.txt")],
        };
        
        let bytes = request.to_bytes().unwrap();
        let deserialized = WatcherRequest::from_bytes(&bytes).unwrap();
        
        match deserialized {
            WatcherRequest::GetMetadata { paths } => {
                assert_eq!(paths.len(), 2);
                assert_eq!(paths[0], PathBuf::from("file1.txt"));
                assert_eq!(paths[1], PathBuf::from("file2.txt"));
            }
            _ => panic!("Wrong request type"),
        }
    }

    #[test]
    fn test_tree_response_serialization() {
        let mut tree = FileSystemTree::new();
        let metadata = FileMetadata {
            size: 100,
            mtime: SystemTime::now(),
            is_symlink: false,
        };
        tree.upsert(&PathBuf::from("test.txt"), Some(metadata));
        
        let response = WatcherResponse::Tree(tree);
        let bytes = response.to_bytes().unwrap();
        let deserialized = WatcherResponse::from_bytes(&bytes).unwrap();
        
        match deserialized {
            WatcherResponse::Tree(tree) => {
                assert!(tree.get_node(&PathBuf::from("test.txt")).is_some());
            }
            _ => panic!("Wrong response type"),
        }
    }

    #[test]
    fn test_metadata_response_serialization() {
        let metadata = FileMetadata {
            size: 200,
            mtime: SystemTime::now(),
            is_symlink: true,
        };
        
        let response = WatcherResponse::Metadata(vec![
            (PathBuf::from("file1.txt"), Some(metadata.clone())),
            (PathBuf::from("dir1"), None),
        ]);
        
        let bytes = response.to_bytes().unwrap();
        let deserialized = WatcherResponse::from_bytes(&bytes).unwrap();
        
        match deserialized {
            WatcherResponse::Metadata(metadata_list) => {
                assert_eq!(metadata_list.len(), 2);
                assert_eq!(metadata_list[0].0, PathBuf::from("file1.txt"));
                assert!(metadata_list[0].1.is_some());
                assert_eq!(metadata_list[1].0, PathBuf::from("dir1"));
                assert!(metadata_list[1].1.is_none());
            }
            _ => panic!("Wrong response type"),
        }
    }

    // Test old protocol compatibility
    #[test]
    fn test_old_status_result_serialization() {
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
    fn test_basic_request_types() {
        let requests = vec![
            WatcherRequest::GetStatus { paths: None },
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