import styled from 'styled-components';

export const StyledInput = styled.input<{ $mono?: boolean }>`
  width: 100%;
  padding: 10px 14px;
  border-radius: 10px;
  background: ${({ theme }) => theme.colors.bgInput};
  border: 1px solid ${({ theme }) => theme.colors.border};
  color: ${({ theme }) => theme.colors.text};
  font-size: 14px;
  font-family: ${({ $mono }) => ($mono ? '"SF Mono", "Fira Code", monospace' : 'inherit')};
  outline: none;
  transition: border-color 0.15s;

  &:focus {
    border-color: ${({ theme }) => theme.colors.borderFocus};
  }

  &::placeholder {
    color: ${({ theme }) => theme.colors.textMuted};
  }
`;

export const Label = styled.label`
  font-size: 12px;
  font-weight: 600;
  color: ${({ theme }) => theme.colors.textMuted};
  margin-bottom: 6px;
  display: block;
  text-transform: uppercase;
  letter-spacing: 0.06em;
`;

export const LabelHint = styled.span`
  font-weight: 400;
  text-transform: none;
`;
