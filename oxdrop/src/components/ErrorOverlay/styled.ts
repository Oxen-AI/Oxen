import styled, { keyframes } from 'styled-components';

const slideUp = keyframes`
  from {
    transform: translateY(20px);
    opacity: 0;
  }
  to {
    transform: translateY(0);
    opacity: 1;
  }
`;

export const Overlay = styled.div`
  position: absolute;
  bottom: 20px;
  left: 20px;
  right: 20px;
  background: ${({ theme }) => theme.colors.bg};
  border: 1px solid ${({ theme }) => theme.colors.border};
  border-radius: 14px;
  padding: 18px;
  box-shadow: ${({ theme }) => theme.shadows.dropdown};
  z-index: 100;
  animation: ${slideUp} 0.3s ease;
`;

export const Content = styled.div`
  display: flex;
  align-items: flex-start;
  gap: 12px;
`;

export const IconBox = styled.div`
  width: 36px;
  height: 36px;
  border-radius: 10px;
  background: ${({ theme }) => theme.colors.dangerSoft};
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
`;

export const Body = styled.div`
  flex: 1;
`;

export const Title = styled.p`
  font-size: 14px;
  font-weight: 500;
  color: ${({ theme }) => theme.colors.text};
`;

export const Message = styled.p`
  font-size: 12px;
  color: ${({ theme }) => theme.colors.textSecondary};
  margin-top: 4px;
  line-height: 1.4;
`;

export const Actions = styled.div`
  display: flex;
  gap: 8px;
  margin-top: 12px;
`;
