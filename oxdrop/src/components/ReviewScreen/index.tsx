import { useState, useCallback } from 'react';
import { convertFileSrc } from '@tauri-apps/api/core';
import { Button } from '@/ui/Button';
import { Input } from '@/ui/Input';
import { RepoPicker } from '@/components/RepoPicker';
import { formatBytes } from '@/lib/format';
import type { IFileScanResult, IRepository } from '@/types';
import * as Styled from './styled';

interface ReviewScreenProps {
  scanResult: IFileScanResult;
  localPaths: string[];  // kept for future path-preserving uploads
  lastUsedRepo?: string;
  onUpload: (repo: string, remotePath: string, message: string) => void;
}

export function ReviewScreen({ scanResult, localPaths: _localPaths, lastUsedRepo, onUpload }: ReviewScreenProps) {
  const [selectedRepo, setSelectedRepo] = useState<string | null>(lastUsedRepo ?? null);
  const [remotePath, setRemotePath] = useState(`/${scanResult.folderName}/`);
  const [description, setDescription] = useState(
    `Added ${scanResult.totalFiles} ${scanResult.detectedTypes[0]?.startsWith('image/') ? 'images' : 'files'} (${formatBytes(scanResult.totalSize)})`,
  );

  const handleRepoChange = useCallback((repo: IRepository) => {
    setSelectedRepo(repo.displayName);
  }, []);

  const handleUpload = () => {
    if (!selectedRepo) return;
    onUpload(selectedRepo, remotePath, description);
  };

  const hasImages = scanResult.thumbnailPaths.length > 0;

  return (
    <Styled.Container>
      <Styled.Title>Ready to upload</Styled.Title>

      <Styled.FileCard>
        <Styled.FileHeader>
          <Styled.FileIcon>📁</Styled.FileIcon>
          <div>
            <Styled.FileName>{scanResult.folderName}/</Styled.FileName>
            <Styled.FileMeta>
              {scanResult.totalFiles} {scanResult.totalFiles === 1 ? 'file' : 'files'} ·{' '}
              {formatBytes(scanResult.totalSize)}
            </Styled.FileMeta>
          </div>
        </Styled.FileHeader>
        {hasImages && (
          <Styled.ThumbnailStrip>
            {scanResult.thumbnailPaths.map((path, i) => (
              <Styled.Thumbnail key={i}>
                <img src={convertFileSrc(path)} alt="" />
              </Styled.Thumbnail>
            ))}
          </Styled.ThumbnailStrip>
        )}
        {!hasImages && scanResult.detectedTypes.length > 0 && (
          <Styled.ThumbnailStrip>
            {scanResult.detectedTypes.slice(0, 6).map((type, i) => (
              <Styled.Thumbnail key={i}>
                <Styled.TypePill>{type.split('/')[1]}</Styled.TypePill>
              </Styled.Thumbnail>
            ))}
          </Styled.ThumbnailStrip>
        )}
      </Styled.FileCard>

      <Styled.Fields>
        <div>
          <Styled.FieldLabel>Upload to</Styled.FieldLabel>
          <RepoPicker value={selectedRepo} onChange={handleRepoChange} />
        </div>
        <Input label="Path" mono value={remotePath} onChange={(e) => setRemotePath(e.target.value)} />
        <Input
          label="Description"
          hint="(optional)"
          value={description}
          onChange={(e) => setDescription(e.target.value)}
        />
      </Styled.Fields>

      <Button onClick={handleUpload} disabled={!selectedRepo} fullWidth style={{ marginTop: 20, padding: 14 }}>
        Upload
      </Button>
    </Styled.Container>
  );
}
