import { open } from '@tauri-apps/plugin-shell';
import { Check } from 'lucide-react';
import { useTheme } from 'styled-components';
import { Button } from '@/ui/Button';
import * as Styled from './styled';

interface UploadCompleteProps {
  repo: string;
  remotePath: string;
  filesUploaded: number;
  host: string;
  scheme: string;
  onUploadMore: () => void;
}

export function UploadComplete({ repo, remotePath, filesUploaded, host, scheme, onUploadMore }: UploadCompleteProps) {
  const theme = useTheme();

  const handleViewHub = () => {
    const trimmed = remotePath.replace(/^\/+|\/+$/g, '');
    const url = trimmed
      ? `${scheme}://${host}/${repo}/dir/main/${trimmed}`
      : `${scheme}://${host}/${repo}`;
    open(url);
  };

  return (
    <Styled.Container>
      <Styled.CheckCircle>
        <Check size={32} color={theme.colors.success} strokeWidth={2.5} />
      </Styled.CheckCircle>
      <Styled.Title>Upload complete</Styled.Title>
      <Styled.Subtitle>
        {filesUploaded} {filesUploaded === 1 ? 'file' : 'files'} uploaded to{' '}
        <Styled.RepoName>{repo}</Styled.RepoName>
      </Styled.Subtitle>
      <Styled.Actions>
        <Button variant="secondary" onClick={handleViewHub}>
          View on OxenHub
        </Button>
        <Button onClick={onUploadMore}>Upload more</Button>
      </Styled.Actions>
    </Styled.Container>
  );
}
