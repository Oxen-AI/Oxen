import { invoke } from '@tauri-apps/api/core';
import type {
  IAuthInfo,
  IFileScanResult,
  IRepository,
  IStartUploadResult,
  IPreferences,
} from '@/types';

export async function checkAuth(): Promise<IAuthInfo | null> {
  return invoke<IAuthInfo | null>('check_auth');
}

export async function login(identifier: string, password: string, host?: string): Promise<IAuthInfo> {
  return invoke<IAuthInfo>('login', { identifier, password, host });
}

export async function saveAuthToken(token: string, host?: string): Promise<void> {
  return invoke('save_auth_token', { token, host });
}

export async function clearAuthToken(): Promise<void> {
  return invoke('clear_auth_token');
}

export async function scanFiles(paths: string[]): Promise<IFileScanResult> {
  return invoke<IFileScanResult>('scan_files', { paths });
}

export async function listRepos(): Promise<IRepository[]> {
  return invoke<IRepository[]>('list_repos');
}

export async function createRepo(namespace: string, name: string): Promise<IRepository> {
  return invoke<IRepository>('create_repo', { namespace, name });
}

export async function startUpload(params: {
  repo: string;
  branch?: string;
  remotePath: string;
  localPaths: string[];
  message: string;
}): Promise<IStartUploadResult> {
  return invoke<IStartUploadResult>('start_upload', params);
}

export async function cancelUpload(jobId: string): Promise<void> {
  return invoke('cancel_upload', { jobId });
}

export async function getPreferences(): Promise<IPreferences> {
  return invoke<IPreferences>('get_preferences');
}

export async function savePreferences(prefs: IPreferences): Promise<void> {
  return invoke('save_preferences', { prefs });
}
