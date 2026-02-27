import styled from 'styled-components';

export const Track = styled.div`
  width: 100%;
  height: 6px;
  background: ${({ theme }) => theme.colors.bgCard};
  border-radius: 3px;
  overflow: hidden;
`;

export const Fill = styled.div<{ $percentage: number }>`
  height: 100%;
  width: ${({ $percentage }) => Math.min($percentage, 100)}%;
  background: ${({ theme }) => theme.gradients.progress};
  border-radius: 3px;
  transition: width 0.3s linear;
`;
