import * as Styled from './styled';

interface ProgressBarProps {
  percentage: number;
}

export function ProgressBar({ percentage }: ProgressBarProps) {
  return (
    <Styled.Track>
      <Styled.Fill $percentage={percentage} />
    </Styled.Track>
  );
}
