import { ProgressBar } from '@/ui/ProgressBar';
import { Button } from '@/ui/Button';
import { formatBytes, formatSpeed, formatDuration } from '@/lib/format';
import * as Styled from './styled';

interface UploadProgressProps {
  repo: string;
  percentage: number;
  bytesUploaded: number;
  bytesTotal: number;
  filesUploaded: number;
  filesTotal: number;
  speed: number;
  eta: number;
  isCommitting: boolean;
  onCancel: () => void;
}

export function UploadProgress({
  repo,
  percentage,
  bytesUploaded,
  bytesTotal,
  filesUploaded,
  filesTotal,
  speed,
  eta,
  isCommitting,
  onCancel,
}: UploadProgressProps) {
  return (
    <Styled.Container>
      <Styled.UploadingTo>
        {isCommitting ? 'Saving snapshot to' : 'Uploading to'}{' '}
        <Styled.RepoName>{repo}</Styled.RepoName>
      </Styled.UploadingTo>

      <Styled.ProgressArea>
        <Styled.PercentageDisplay>
          <Styled.PercentageNumber>{Math.floor(percentage)}</Styled.PercentageNumber>
          <Styled.PercentageSign>%</Styled.PercentageSign>
        </Styled.PercentageDisplay>

        <ProgressBar percentage={percentage} />

        <Styled.Stats>
          <span>
            {filesUploaded} / {filesTotal} files
          </span>
          <span>
            {formatBytes(bytesUploaded)} / {formatBytes(bytesTotal)}
          </span>
        </Styled.Stats>

        <Styled.SubStats>
          <span>{speed > 0 ? formatSpeed(speed) : ''}</span>
          <span>{eta > 0 ? `${formatDuration(eta)} remaining` : ''}</span>
        </Styled.SubStats>

        {isCommitting && <Styled.CommittingText>Saving snapshot...</Styled.CommittingText>}
      </Styled.ProgressArea>

      {!isCommitting && (
        <Button variant="danger" onClick={onCancel} style={{ marginTop: 12 }}>
          Cancel
        </Button>
      )}
    </Styled.Container>
  );
}
