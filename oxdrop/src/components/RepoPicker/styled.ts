import styled from 'styled-components';

export const Container = styled.div`
  position: relative;
`;

export const Trigger = styled.div<{ $open: boolean }>`
  padding: 10px 14px;
  border-radius: 10px;
  background: ${({ theme }) => theme.colors.bgInput};
  border: 1px solid ${({ theme, $open }) => ($open ? theme.colors.borderFocus : theme.colors.border)};
  color: ${({ theme }) => theme.colors.text};
  font-size: 14px;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: space-between;
  transition: border-color 0.15s;
`;

export const Dropdown = styled.div`
  position: absolute;
  top: calc(100% + 4px);
  left: 0;
  right: 0;
  background: ${({ theme }) => theme.colors.bg};
  border: 1px solid ${({ theme }) => theme.colors.border};
  border-radius: 10px;
  padding: 4px;
  z-index: 10;
  box-shadow: ${({ theme }) => theme.shadows.dropdown};
  max-height: 240px;
  overflow-y: auto;
`;

export const SearchInput = styled.input`
  width: 100%;
  padding: 8px 12px;
  border: none;
  border-bottom: 1px solid ${({ theme }) => theme.colors.border};
  background: transparent;
  color: ${({ theme }) => theme.colors.text};
  font-size: 13px;
  outline: none;
  margin-bottom: 4px;

  &::placeholder {
    color: ${({ theme }) => theme.colors.textMuted};
  }
`;

export const Option = styled.div<{ $selected: boolean }>`
  padding: 8px 12px;
  border-radius: 6px;
  font-size: 13px;
  color: ${({ theme, $selected }) => ($selected ? theme.colors.accent : theme.colors.text)};
  cursor: pointer;
  background: ${({ theme, $selected }) => ($selected ? theme.colors.accentSoft : 'transparent')};
  transition: background 0.1s;

  &:hover {
    background: ${({ theme, $selected }) => ($selected ? theme.colors.accentSoft : theme.colors.bgCardHover)};
  }
`;

export const Arrow = styled.span`
  color: ${({ theme }) => theme.colors.textMuted};
  font-size: 11px;
`;

export const Row = styled.div`
  display: flex;
  gap: 8px;
`;

export const EmptyText = styled.p`
  font-size: 13px;
  color: ${({ theme }) => theme.colors.textMuted};
  padding: 12px;
  text-align: center;
`;
