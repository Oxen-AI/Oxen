import { useState, useCallback, useEffect } from 'react';
import { ThemeProvider } from 'styled-components';
import { GlobalStyle } from '@/styles/GlobalStyle';
import { darkTheme, lightTheme } from '@/styles/themes';
import { useSystemTheme } from '@/hooks/useSystemTheme';
import { useAuth } from '@/hooks/useAuth';
import { useUpload } from '@/hooks/useUpload';
import { AuthProvider } from '@/context/AuthContext';
import { TitleBar } from '@/ui/TitleBar';
import { SignIn } from '@/components/SignIn';
import { DropZone } from '@/components/DropZone';
import { ReviewScreen } from '@/components/ReviewScreen';
import { UploadProgress } from '@/components/UploadProgress';
import { UploadComplete } from '@/components/UploadComplete';
import { ErrorOverlay } from '@/components/ErrorOverlay';
import * as commands from '@/lib/commands';
import type { Screen, IFileScanResult, IRecentUpload, IPreferences } from '@/types';

function AppContent() {
  const systemTheme = useSystemTheme();
  const theme = systemTheme === 'dark' ? darkTheme : lightTheme;
  const { isLoading, isAuthenticated, host } = useAuth();
  const upload = useUpload();

  const [screen, setScreen] = useState<Screen>('signin');
  const [scanResult, setScanResult] = useState<IFileScanResult | null>(null);
  const [localPaths, setLocalPaths] = useState<string[]>([]);
  const [uploadRepo, setUploadRepo] = useState<string>('');
  const [uploadRemotePath, setUploadRemotePath] = useState<string>('');
  const [preferences, setPreferences] = useState<IPreferences>({ recentUploads: [] });
  const [error, setError] = useState<{ title: string; message: string } | null>(null);
  const [jobId, setJobId] = useState<string | null>(null);

  // Load preferences on mount
  useEffect(() => {
    commands.getPreferences().then(setPreferences).catch(() => {});
  }, []);

  // Navigate to dropzone when auth is resolved
  useEffect(() => {
    if (!isLoading && isAuthenticated) {
      setScreen('dropzone');
    } else if (!isLoading && !isAuthenticated) {
      setScreen('signin');
    }
  }, [isLoading, isAuthenticated]);

  // Watch upload progress for screen transitions
  useEffect(() => {
    if (!upload.progress) return;

    if (upload.progress.status === 'complete') {
      setScreen('complete');
      // Save to recent uploads
      if (scanResult) {
        const recent: IRecentUpload = {
          folderName: scanResult.folderName,
          repo: uploadRepo,
          remotePath: `/${scanResult.folderName}/`,
          fileCount: scanResult.totalFiles,
          totalSize: scanResult.totalSize,
          timestamp: new Date().toISOString(),
        };
        const newPrefs = {
          ...preferences,
          recentUploads: [recent, ...preferences.recentUploads].slice(0, 20),
          lastUsedRepo: uploadRepo,
        };
        setPreferences(newPrefs);
        commands.savePreferences(newPrefs).catch(() => {});
      }
    } else if (upload.progress.status === 'failed') {
      setError({
        title: 'Upload failed',
        message: upload.progress.error || 'Something went wrong. Please try again.',
      });
    } else if (upload.progress.status === 'cancelled') {
      setScreen('dropzone');
      upload.reset();
    }
  }, [upload.progress?.status]);

  const handleFilesSelected = useCallback(
    async (paths: string[]) => {
      try {
        const result = await commands.scanFiles(paths);
        setScanResult(result);
        setLocalPaths(paths);
        setScreen('review');
      } catch {
        setError({ title: 'Scan failed', message: 'Could not read the selected files.' });
      }
    },
    [],
  );

  const handleUpload = useCallback(
    async (repo: string, remotePath: string, message: string) => {
      setUploadRepo(repo);
      setUploadRemotePath(remotePath);
      setScreen('progress');
      upload.reset();
      try {
        const result = await commands.startUpload({
          repo,
          remotePath,
          localPaths,
          message,
        });
        setJobId(result.jobId);
      } catch {
        setError({ title: 'Upload failed', message: 'Could not start the upload.' });
        setScreen('review');
      }
    },
    [localPaths, upload],
  );

  const handleCancel = useCallback(async () => {
    if (jobId) {
      await commands.cancelUpload(jobId);
    }
  }, [jobId]);

  const handleUploadMore = useCallback(() => {
    setScanResult(null);
    setLocalPaths([]);
    setJobId(null);
    upload.reset();
    setScreen('dropzone');
  }, [upload]);

  const showBack = screen === 'review';

  // Map API host to web UI host (they run separately)
  const webScheme = host === 'localhost:3001' ? 'http' : 'https';
  const webHost = host === 'localhost:3001' ? 'localhost:3002' : 'oxen.ai';

  return (
    <ThemeProvider theme={theme}>
      <GlobalStyle />
      <div style={{ display: 'flex', flexDirection: 'column', height: '100vh', position: 'relative' }}>
        <TitleBar showBack={showBack} onBack={() => setScreen('dropzone')} />
        <div style={{ flex: 1, overflow: 'auto' }}>
          {screen === 'signin' && <SignIn onSignedIn={() => setScreen('dropzone')} />}
          {screen === 'dropzone' && (
            <DropZone recentUploads={preferences.recentUploads} onFilesSelected={handleFilesSelected} />
          )}
          {screen === 'review' && scanResult && (
            <ReviewScreen
              scanResult={scanResult}
              localPaths={localPaths}
              lastUsedRepo={preferences.lastUsedRepo}
              onUpload={handleUpload}
            />
          )}
          {screen === 'progress' && (
            <UploadProgress
              repo={uploadRepo}
              percentage={upload.percentage}
              bytesUploaded={upload.progress?.bytesUploaded ?? 0}
              bytesTotal={upload.progress?.bytesTotal ?? 0}
              filesUploaded={upload.progress?.filesUploaded ?? 0}
              filesTotal={upload.progress?.filesTotal ?? 0}
              speed={upload.speed}
              eta={upload.eta}
              isCommitting={upload.progress?.status === 'committing'}
              onCancel={handleCancel}
            />
          )}
          {screen === 'complete' && (
            <UploadComplete
              repo={uploadRepo}
              remotePath={uploadRemotePath}
              filesUploaded={upload.progress?.filesTotal ?? 0}
              host={webHost}
              scheme={webScheme}
              onUploadMore={handleUploadMore}
            />
          )}
        </div>
        {error && (
          <ErrorOverlay
            title={error.title}
            message={error.message}
            onRetry={
              screen === 'progress'
                ? () => {
                    setError(null);
                  }
                : undefined
            }
            onDismiss={() => setError(null)}
          />
        )}
      </div>
    </ThemeProvider>
  );
}

export default function App() {
  return (
    <AuthProvider>
      <AppContent />
    </AuthProvider>
  );
}
