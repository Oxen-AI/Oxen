import styled, { keyframes } from 'styled-components';

const float = keyframes`
  0%, 100% { transform: translateY(0px); }
  50% { transform: translateY(-8px); }
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

export const Logo = styled.div`
  font-size: 56px;
  margin-bottom: 24px;
  filter: drop-shadow(0 4px 24px rgba(91, 141, 239, 0.15));
  animation: ${float} 3s ease-in-out infinite;
`;

export const Title = styled.h1`
  font-size: 28px;
  font-weight: 700;
  color: ${({ theme }) => theme.colors.text};
  letter-spacing: -0.03em;
`;

export const Subtitle = styled.p`
  font-size: 15px;
  color: ${({ theme }) => theme.colors.textSecondary};
  margin-top: 10px;
  margin-bottom: 36px;
  line-height: 1.5;
  max-width: 300px;
`;

export const SignInButton = styled.button`
  padding: 14px 40px;
  border-radius: 12px;
  font-size: 15px;
  font-weight: 600;
  cursor: pointer;
  border: none;
  color: #fff;
  background: ${({ theme }) => theme.gradients.cta};
  box-shadow: 0 4px 20px rgba(91, 141, 239, 0.2);
  transition: all 0.3s ease;

  &:hover {
    transform: translateY(-1px);
    box-shadow: 0 8px 32px rgba(91, 141, 239, 0.35);
  }
`;

export const Hint = styled.p`
  font-size: 12px;
  color: ${({ theme }) => theme.colors.textMuted};
  margin-top: 20px;
`;

export const TokenForm = styled.div`
  width: 100%;
  max-width: 320px;
  display: flex;
  flex-direction: column;
  gap: 12px;
  margin-top: 24px;
`;

export const ErrorText = styled.p`
  font-size: 13px;
  color: ${({ theme }) => theme.colors.danger};
  margin-top: 8px;
`;

export const ToggleLink = styled.button`
  background: none;
  border: none;
  color: ${({ theme }) => theme.colors.textSecondary};
  font-size: 13px;
  cursor: pointer;
  padding: 4px 0;

  &:hover {
    text-decoration: underline;
    color: ${({ theme }) => theme.colors.text};
  }
`;
