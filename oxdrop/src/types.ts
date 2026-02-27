export interface IUser {
  name: string;
  email: string;
}

export interface IAuthInfo {
  token: string;
  user: IUser;
  host: string;
}

export interface IRepository {
  namespace: string;
  name: string;
  displayName: string;
}

export interface IFileScanResult {
  totalFiles: number;
  totalSize: number;
  folderName: string;
  detectedTypes: string[];
  thumbnailPaths: string[];
}

export interface IUploadProgressEvent {
  jobId: string;
  status: 'uploading' | 'committing' | 'complete' | 'failed' | 'cancelled';
  bytesUploaded: number;
  bytesTotal: number;
  filesUploaded: number;
  filesTotal: number;
  currentFile: string;
  error?: string;
}

export interface IPreferences {
  recentUploads: IRecentUpload[];
  lastUsedRepo?: string;
}

export interface IRecentUpload {
  folderName: string;
  repo: string;
  remotePath: string;
  fileCount: number;
  totalSize: number;
  timestamp: string;
}

export interface IStartUploadResult {
  jobId: string;
}

export type Screen = 'signin' | 'dropzone' | 'review' | 'progress' | 'complete';
