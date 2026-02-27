import { useState, useEffect } from 'react';
import { getCurrentWindow } from '@tauri-apps/api/window';
import { open } from '@tauri-apps/plugin-dialog';
import { Upload } from 'lucide-react';
import { useTheme } from 'styled-components';
import { formatBytes, formatTimeAgo } from '@/lib/format';
import type { IRecentUpload } from '@/types';
import * as Styled from './styled';

interface DropZoneProps {
  recentUploads: IRecentUpload[];
  onFilesSelected: (paths: string[]) => void;
}

export function DropZone({ recentUploads, onFilesSelected }: DropZoneProps) {
  const [isDragging, setIsDragging] = useState(false);
  const theme = useTheme();

  useEffect(() => {
    const appWindow = getCurrentWindow();

    const unlistenEnter = appWindow.onDragDropEvent((event) => {
      if (event.payload.type === 'enter') {
        setIsDragging(true);
      } else if (event.payload.type === 'leave') {
        setIsDragging(false);
      } else if (event.payload.type === 'drop') {
        setIsDragging(false);
        const paths = event.payload.paths;
        if (paths.length > 0) {
          onFilesSelected(paths);
        }
      }
    });

    return () => {
      unlistenEnter.then((fn) => fn());
    };
  }, [onFilesSelected]);

  const handleBrowse = async () => {
    const selected = await open({
      multiple: true,
      directory: true,
    });
    if (selected) {
      const paths = Array.isArray(selected) ? selected : [selected];
      if (paths.length > 0) {
        onFilesSelected(paths);
      }
    }
  };

  return (
    <Styled.Container>
      <Styled.Zone $isDragging={isDragging} onClick={handleBrowse}>
        <Styled.IconBox>
          <Upload size={24} color={theme.colors.accent} />
        </Styled.IconBox>
        <Styled.ZoneTitle>Drop files or folders here</Styled.ZoneTitle>
        <Styled.ZoneHint>or click to browse</Styled.ZoneHint>
      </Styled.Zone>

      {recentUploads.length > 0 && (
        <Styled.RecentSection>
          <Styled.RecentTitle>Recent uploads</Styled.RecentTitle>
          {recentUploads.slice(0, 5).map((item, i) => (
            <Styled.RecentItem key={i}>
              <Styled.RecentItemLeft>
                <Styled.RecentIcon>📁</Styled.RecentIcon>
                <div>
                  <Styled.RecentName>{item.folderName}</Styled.RecentName>
                  <Styled.RecentMeta>
                    {item.fileCount} files · {formatBytes(item.totalSize)} → {item.repo}
                  </Styled.RecentMeta>
                </div>
              </Styled.RecentItemLeft>
              <Styled.RecentTime>{formatTimeAgo(item.timestamp)}</Styled.RecentTime>
            </Styled.RecentItem>
          ))}
        </Styled.RecentSection>
      )}
    </Styled.Container>
  );
}
