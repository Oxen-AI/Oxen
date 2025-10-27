use crate::view::Pagination;

/// Returns a vector of entries and the total number of pages.
/// Note: does this in memory, so not as efficient as down at the db level, but rocksdb does not support pagination
pub fn paginate<T: Clone>(entries: Vec<T>, page: usize, page_size: usize) -> (Vec<T>, Pagination) {
    let total = entries.len();
    paginate_with_total(entries, page, page_size, total)
}

/// Returns a vector of entries and the total number of pages.
pub fn paginate_with_total<T: Clone>(
    entries: Vec<T>,
    page_number: usize,
    page_size: usize,
    total_entries: usize,
) -> (Vec<T>, Pagination) {
    let total_pages = (total_entries as f64 / page_size as f64).ceil() as usize;
    log::debug!(
        "paginate entries page: {page_number} size: {page_size} total: {total_entries} total_pages: {total_pages}",
    );

    let start = if page_number == 0 {
        page_number * page_size
    } else {
        (page_number - 1) * page_size
    };
    let end = start + page_size;

    log::debug!(
        "paginate entries start: {} end: {} total: {}",
        start,
        end,
        entries.len()
    );

    let pagination = Pagination {
        page_size,
        page_number,
        total_pages,
        total_entries,
    };

    if start < entries.len() && end > entries.len() {
        (entries[start..].to_vec(), pagination)
    } else if start < entries.len() && end <= entries.len() {
        (entries[start..end].to_vec(), pagination)
    } else {
        (vec![], pagination)
    }
}

/// Paginate directories and entries putting the directories on the top, makeing sure we respect page and page_size for the combination of both
/// We have the object separated in our db so this makes for more efficient pagination
pub fn paginate_dirs_and_files<T: Clone>(
    dirs: &[T],
    files: &[T],
    page_number: usize,
    page_size: usize,
) -> (Vec<T>, Pagination) {
    let total_entries = dirs.len() + files.len();
    let start_idx = if page_number == 0 {
        page_number * page_size
    } else {
        (page_number - 1) * page_size
    };
    let total_pages = (total_entries as f64 / page_size as f64).ceil() as usize;
    if start_idx >= total_entries {
        let pagination = Pagination {
            page_size: 0,
            page_number,
            total_pages,
            total_entries,
        };
        return (Vec::new(), pagination);
    }

    let start_a = start_idx.min(dirs.len());
    let end_a = start_a + page_size.min(dirs.len() - start_a);

    let mut result: Vec<T> = Vec::new();

    result.extend_from_slice(&dirs[start_a..end_a]);

    let remaining_space = page_size - (end_a - start_a);
    if remaining_space > 0 {
        // Compute where to start and end for the files
        let start_b = if start_idx < dirs.len() {
            0
        } else {
            start_idx - dirs.len()
        };
        let end_b = start_b + remaining_space.min(files.len() - start_b);
        result.extend_from_slice(&files[start_b..end_b]);
    }

    let pagination = Pagination {
        page_size,
        page_number,
        total_pages,
        total_entries,
    };

    (result, pagination)
}

pub fn paginate_dirs_assuming_files<T: Clone>(
    dirs: &[T],
    num_files: usize,
    page_number: usize,
    page_size: usize,
) -> (Vec<T>, Pagination) {
    let total_entries = dirs.len() + num_files;
    let start_idx = if page_number == 0 {
        page_number * page_size
    } else {
        (page_number - 1) * page_size
    };
    let total_pages = (total_entries as f64 / page_size as f64).ceil() as usize;
    if start_idx >= total_entries {
        let pagination = Pagination {
            page_size: 0,
            page_number,
            total_pages,
            total_entries,
        };
        return (Vec::new(), pagination);
    }

    let start_a = start_idx.min(dirs.len());
    let end_a = start_a + page_size.min(dirs.len() - start_a);

    let mut result: Vec<T> = Vec::new();

    result.extend_from_slice(&dirs[start_a..end_a]);

    let pagination = Pagination {
        page_size,
        page_number,
        total_pages,
        total_entries,
    };

    (result, pagination)
}

pub fn paginate_files_assuming_dirs<T: Clone>(
    files: &[T],
    num_dirs: usize,
    page_number: usize,
    page_size: usize,
) -> (Vec<T>, Pagination) {
    let total_entries = num_dirs + files.len();
    let start_idx = if page_number == 0 {
        page_number * page_size
    } else {
        (page_number - 1) * page_size
    };
    let total_pages = (total_entries as f64 / page_size as f64).ceil() as usize;
    if start_idx >= total_entries {
        let pagination = Pagination {
            page_size: 0,
            page_number,
            total_pages,
            total_entries,
        };
        return (Vec::new(), pagination);
    }

    let start_a = start_idx.min(num_dirs);
    let end_a = start_a + page_size.min(num_dirs - start_a);

    let mut result: Vec<T> = Vec::new();

    let remaining_space = page_size - (end_a - start_a);
    if remaining_space > 0 {
        // Compute where to start and end for the files
        let start_b = start_idx.saturating_sub(num_dirs);
        let end_b = start_b + remaining_space.min(files.len() - start_b);
        result.extend_from_slice(&files[start_b..end_b]);
    }

    let pagination = Pagination {
        page_size,
        page_number,
        total_pages,
        total_entries,
    };

    (result, pagination)
}

#[cfg(test)]
mod tests {
    use super::paginate_dirs_and_files;
    use std::path::PathBuf;

    #[test]
    fn test_paginate_dirs_files_both_lists_empty() {
        let dirs: Vec<PathBuf> = Vec::new();
        let files: Vec<PathBuf> = Vec::new();
        let (entries, _) = paginate_dirs_and_files(&dirs, &files, 0, 5);

        assert_eq!(entries, Vec::<PathBuf>::new());
    }

    #[test]
    fn test_paginate_dirs_files_first_list_empty() {
        let dirs: Vec<PathBuf> = Vec::new();
        let files = vec![
            PathBuf::from("file1"),
            PathBuf::from("file2"),
            PathBuf::from("file3"),
        ];
        let (entries, _) = paginate_dirs_and_files(&dirs, &files, 0, 5);
        assert_eq!(entries, files);
    }

    #[test]
    fn test_paginate_dirs_files_second_list_empty() {
        let dirs = vec![
            PathBuf::from("dir1"),
            PathBuf::from("dir2"),
            PathBuf::from("dir3"),
        ];
        let files: Vec<PathBuf> = Vec::new();
        let (entries, _) = paginate_dirs_and_files(&dirs, &files, 0, 5);
        assert_eq!(entries, dirs);
    }

    #[test]
    fn test_paginate_dirs_files_page_size_less_than_dirs_length() {
        let dirs = vec![
            PathBuf::from("dir1"),
            PathBuf::from("dir2"),
            PathBuf::from("dir3"),
            PathBuf::from("dir4"),
        ];
        let files = vec![
            PathBuf::from("file1"),
            PathBuf::from("file2"),
            PathBuf::from("file3"),
        ];
        let (entries, _) = paginate_dirs_and_files(&dirs, &files, 0, 2);
        assert_eq!(entries, vec![PathBuf::from("dir1"), PathBuf::from("dir2")]);
    }

    #[test]
    fn test_paginate_dirs_files_page_size_more_than_dirs_length() {
        let dirs = vec![
            PathBuf::from("dir1"),
            PathBuf::from("dir2"),
            PathBuf::from("dir3"),
        ];
        let files = vec![
            PathBuf::from("file1"),
            PathBuf::from("file2"),
            PathBuf::from("file3"),
        ];
        let (entries, _) = paginate_dirs_and_files(&dirs, &files, 0, 5);
        assert_eq!(
            entries,
            vec![
                PathBuf::from("dir1"),
                PathBuf::from("dir2"),
                PathBuf::from("dir3"),
                PathBuf::from("file1"),
                PathBuf::from("file2")
            ]
        );
    }

    #[test]
    fn test_paginate_dirs_files_page_size_more_than_total_length() {
        let dirs = vec![
            PathBuf::from("dir1"),
            PathBuf::from("dir2"),
            PathBuf::from("dir3"),
        ];
        let files = vec![
            PathBuf::from("file1"),
            PathBuf::from("file2"),
            PathBuf::from("file3"),
        ];
        let (entries, _) = paginate_dirs_and_files(&dirs, &files, 0, 10);

        assert_eq!(
            entries,
            vec![
                PathBuf::from("dir1"),
                PathBuf::from("dir2"),
                PathBuf::from("dir3"),
                PathBuf::from("file1"),
                PathBuf::from("file2"),
                PathBuf::from("file3")
            ]
        );
    }

    #[test]
    fn test_paginate_dirs_files_page_number_out_of_range() {
        let dirs = vec![
            PathBuf::from("dir1"),
            PathBuf::from("dir2"),
            PathBuf::from("dir3"),
        ];
        let files = vec![
            PathBuf::from("file1"),
            PathBuf::from("file2"),
            PathBuf::from("file3"),
        ];
        let (entries, _) = paginate_dirs_and_files(&dirs, &files, 10, 5);
        assert_eq!(entries, Vec::<PathBuf>::new());
    }

    #[test]
    fn test_paginate_dirs_files_combine_multiple_pages() {
        let dirs = vec![
            PathBuf::from("dir1"),
            PathBuf::from("dir2"),
            PathBuf::from("dir3"),
            PathBuf::from("dir4"),
        ];
        let files = vec![
            PathBuf::from("file1"),
            PathBuf::from("file2"),
            PathBuf::from("file3"),
            PathBuf::from("file4"),
        ];
        let (entries, _) = paginate_dirs_and_files(&dirs, &files, 1, 3);
        assert_eq!(
            entries,
            vec![
                PathBuf::from("dir1"),
                PathBuf::from("dir2"),
                PathBuf::from("dir3")
            ]
        );
        let (entries, _) = paginate_dirs_and_files(&dirs, &files, 2, 3);
        assert_eq!(
            entries,
            vec![
                PathBuf::from("dir4"),
                PathBuf::from("file1"),
                PathBuf::from("file2")
            ]
        );
        let (entries, _) = paginate_dirs_and_files(&dirs, &files, 3, 3);
        assert_eq!(
            entries,
            vec![PathBuf::from("file3"), PathBuf::from("file4")]
        );
    }
}
