#[cfg(test)]
mod tests {
    use crate::protocol::*;
    use crate::tree::{FileMetadata, FileSystemTree};
    use std::path::PathBuf;
    use std::time::SystemTime;

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

    #[test]
    fn test_basic_request_types() {
        let requests = vec![WatcherRequest::Shutdown, WatcherRequest::Ping];

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
    fn test_large_tree_payload() {
        // Test with a tree containing many files
        let mut tree = FileSystemTree::new();

        for i in 0..1000 {
            let path = PathBuf::from(format!("file{}.txt", i));
            let metadata = FileMetadata {
                size: i as u64,
                mtime: SystemTime::now(),
                is_symlink: false,
            };
            tree.upsert(&path, Some(metadata));
        }

        let response = WatcherResponse::Tree(tree);
        let bytes = response.to_bytes().unwrap();
        let deserialized = WatcherResponse::from_bytes(&bytes).unwrap();

        match deserialized {
            WatcherResponse::Tree(result_tree) => {
                // Check some sample files
                assert!(result_tree.get_node(&PathBuf::from("file0.txt")).is_some());
                assert!(result_tree
                    .get_node(&PathBuf::from("file999.txt"))
                    .is_some());

                // Count total files
                let file_count = result_tree.iter_files().count();
                assert_eq!(file_count, 1000);
            }
            _ => panic!("Wrong response type"),
        }
    }
}
