import styled from 'styled-components';

export const Container = styled.div`
  display: flex;
  flex-direction: column;
  height: 100%;
  padding: 24px;
`;

export const Zone = styled.div<{ $isDragging: boolean }>`
  flex: 1;
  min-height: 220px;
  border: 2px dashed ${({ theme, $isDragging }) => ($isDragging ? theme.colors.accent : theme.colors.border)};
  border-radius: 16px;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  background: ${({ theme, $isDragging }) => ($isDragging ? theme.colors.accentSoft : theme.colors.dropzoneBg)};
  transition: all 0.25s ease;

  &:hover {
    border-color: ${({ theme }) => theme.colors.accent};
    background: ${({ theme }) => theme.colors.accentSoft};
  }
`;

export const IconBox = styled.div`
  width: 56px;
  height: 56px;
  border-radius: 14px;
  background: ${({ theme }) => theme.colors.accentSoft};
  display: flex;
  align-items: center;
  justify-content: center;
  margin-bottom: 16px;
`;

export const ZoneTitle = styled.p`
  font-size: 16px;
  font-weight: 500;
  color: ${({ theme }) => theme.colors.text};
`;

export const ZoneHint = styled.p`
  font-size: 13px;
  color: ${({ theme }) => theme.colors.textSecondary};
  margin-top: 6px;
`;

export const RecentSection = styled.div`
  margin-top: 24px;
`;

export const RecentTitle = styled.p`
  font-size: 12px;
  font-weight: 600;
  color: ${({ theme }) => theme.colors.textMuted};
  text-transform: uppercase;
  letter-spacing: 0.06em;
  margin-bottom: 10px;
`;

export const RecentItem = styled.div<{ $active?: boolean }>`
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 10px 14px;
  border-radius: 10px;
  cursor: pointer;
  transition: background 0.15s;

  &:hover {
    background: ${({ theme }) => theme.colors.bgCardHover};
  }
`;

export const RecentItemLeft = styled.div`
  display: flex;
  align-items: center;
  gap: 10px;
`;

export const RecentIcon = styled.div`
  width: 32px;
  height: 32px;
  border-radius: 8px;
  background: ${({ theme }) => theme.colors.bgCard};
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 14px;
`;

export const RecentName = styled.p`
  font-size: 13px;
  font-weight: 500;
  color: ${({ theme }) => theme.colors.text};
`;

export const RecentMeta = styled.p`
  font-size: 11px;
  color: ${({ theme }) => theme.colors.textMuted};
  margin-top: 2px;
`;

export const RecentTime = styled.span`
  font-size: 11px;
  color: ${({ theme }) => theme.colors.textMuted};
`;

export const HiddenInput = styled.input`
  display: none;
`;
