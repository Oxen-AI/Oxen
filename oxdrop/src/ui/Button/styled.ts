import styled, { css } from 'styled-components';

export type ButtonVariant = 'primary' | 'secondary' | 'ghost' | 'danger';

export const StyledButton = styled.button<{ $variant: ButtonVariant; $fullWidth?: boolean }>`
  padding: 10px 24px;
  border-radius: 10px;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s ease;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 6px;

  ${({ $fullWidth }) =>
    $fullWidth &&
    css`
      width: 100%;
    `}

  &:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  ${({ $variant, theme }) => {
    switch ($variant) {
      case 'primary':
        return css`
          background: ${theme.colors.accent};
          color: #fff;
          border: none;
          &:hover:not(:disabled) {
            background: ${theme.colors.accentHover};
          }
        `;
      case 'secondary':
        return css`
          background: transparent;
          color: ${theme.colors.text};
          border: 1px solid ${theme.colors.border};
          &:hover:not(:disabled) {
            background: ${theme.colors.bgCardHover};
          }
        `;
      case 'ghost':
        return css`
          background: transparent;
          color: ${theme.colors.accent};
          border: none;
          &:hover:not(:disabled) {
            background: ${theme.colors.accentSoft};
          }
        `;
      case 'danger':
        return css`
          background: transparent;
          color: ${theme.colors.danger};
          border: 1px solid ${theme.colors.border};
          &:hover:not(:disabled) {
            border-color: ${theme.colors.danger};
            background: ${theme.colors.dangerSoft};
          }
        `;
    }
  }}
`;
