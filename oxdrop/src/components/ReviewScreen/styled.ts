import styled from 'styled-components';

export const Container = styled.div`
  display: flex;
  flex-direction: column;
  height: 100%;
  padding: 24px;
`;

export const Title = styled.h2`
  font-size: 20px;
  font-weight: 600;
  color: ${({ theme }) => theme.colors.text};
  letter-spacing: -0.02em;
  margin-bottom: 20px;
`;

export const FileCard = styled.div`
  background: ${({ theme }) => theme.colors.bgCard};
  border-radius: 14px;
  padding: 18px;
  margin-bottom: 24px;
  border: 1px solid ${({ theme }) => theme.colors.border};
`;

export const FileHeader = styled.div`
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 14px;
`;

export const FileIcon = styled.span`
  font-size: 18px;
`;

export const FileName = styled.p`
  font-size: 14px;
  font-weight: 500;
  color: ${({ theme }) => theme.colors.text};
`;

export const FileMeta = styled.p`
  font-size: 12px;
  color: ${({ theme }) => theme.colors.textSecondary};
  margin-top: 2px;
`;

export const ThumbnailStrip = styled.div`
  display: flex;
  gap: 6px;
  overflow: hidden;
`;

export const Thumbnail = styled.div`
  width: 52px;
  height: 52px;
  border-radius: 8px;
  background: ${({ theme }) => theme.colors.bgCardHover};
  flex-shrink: 0;
  overflow: hidden;

  img {
    width: 100%;
    height: 100%;
    object-fit: cover;
  }
`;

export const TypePill = styled.span`
  font-size: 10px;
  color: ${({ theme }) => theme.colors.textMuted};
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
`;

export const Fields = styled.div`
  display: flex;
  flex-direction: column;
  gap: 16px;
  flex: 1;
`;

export const FieldLabel = styled.label`
  font-size: 12px;
  font-weight: 600;
  color: ${({ theme }) => theme.colors.textMuted};
  margin-bottom: 6px;
  display: block;
  text-transform: uppercase;
  letter-spacing: 0.06em;
`;
