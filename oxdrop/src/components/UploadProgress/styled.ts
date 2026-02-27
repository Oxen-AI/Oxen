import styled from 'styled-components';

export const Container = styled.div`
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  height: 100%;
  padding: 40px;
`;

export const UploadingTo = styled.p`
  font-size: 14px;
  color: ${({ theme }) => theme.colors.textSecondary};
  margin-bottom: 6px;
`;

export const RepoName = styled.span`
  color: ${({ theme }) => theme.colors.text};
  font-weight: 500;
`;

export const ProgressArea = styled.div`
  width: 100%;
  max-width: 340px;
  margin-top: 28px;
  margin-bottom: 20px;
`;

export const PercentageDisplay = styled.div`
  display: flex;
  align-items: baseline;
  justify-content: center;
  margin-bottom: 16px;
`;

export const PercentageNumber = styled.span`
  font-size: 48px;
  font-weight: 700;
  color: ${({ theme }) => theme.colors.text};
  letter-spacing: -0.04em;
  font-variant-numeric: tabular-nums;
`;

export const PercentageSign = styled.span`
  font-size: 20px;
  color: ${({ theme }) => theme.colors.textMuted};
  font-weight: 500;
  margin-left: 2px;
`;

export const Stats = styled.div`
  display: flex;
  justify-content: space-between;
  margin-top: 14px;
  font-size: 12px;
  color: ${({ theme }) => theme.colors.textSecondary};
  font-variant-numeric: tabular-nums;
`;

export const SubStats = styled.div`
  display: flex;
  justify-content: space-between;
  margin-top: 4px;
  font-size: 12px;
  color: ${({ theme }) => theme.colors.textMuted};
`;

export const CommittingText = styled.p`
  font-size: 14px;
  color: ${({ theme }) => theme.colors.textSecondary};
  margin-top: 16px;
`;
