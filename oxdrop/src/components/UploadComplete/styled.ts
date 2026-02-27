import styled, { keyframes } from 'styled-components';

const scaleIn = keyframes`
  from {
    transform: scale(0.5);
    opacity: 0;
  }
  to {
    transform: scale(1);
    opacity: 1;
  }
`;

export const Container = styled.div`
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  height: 100%;
  padding: 40px;
  text-align: center;
`;

export const CheckCircle = styled.div`
  width: 72px;
  height: 72px;
  border-radius: 50%;
  background: ${({ theme }) => theme.colors.successSoft};
  display: flex;
  align-items: center;
  justify-content: center;
  margin-bottom: 24px;
  animation: ${scaleIn} 0.4s cubic-bezier(0.34, 1.56, 0.64, 1);
`;

export const Title = styled.h2`
  font-size: 22px;
  font-weight: 600;
  color: ${({ theme }) => theme.colors.text};
  letter-spacing: -0.02em;
`;

export const Subtitle = styled.p`
  font-size: 14px;
  color: ${({ theme }) => theme.colors.textSecondary};
  margin-top: 8px;
  margin-bottom: 32px;
`;

export const RepoName = styled.span`
  color: ${({ theme }) => theme.colors.text};
  font-weight: 500;
`;

export const Actions = styled.div`
  display: flex;
  gap: 10px;
`;
