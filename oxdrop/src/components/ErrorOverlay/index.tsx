import { AlertTriangle } from 'lucide-react';
import { useTheme } from 'styled-components';
import { Button } from '@/ui/Button';
import * as Styled from './styled';

interface ErrorOverlayProps {
  title: string;
  message: string;
  onRetry?: () => void;
  onDismiss: () => void;
}

export function ErrorOverlay({ title, message, onRetry, onDismiss }: ErrorOverlayProps) {
  const theme = useTheme();

  return (
    <Styled.Overlay>
      <Styled.Content>
        <Styled.IconBox>
          <AlertTriangle size={16} color={theme.colors.danger} />
        </Styled.IconBox>
        <Styled.Body>
          <Styled.Title>{title}</Styled.Title>
          <Styled.Message>{message}</Styled.Message>
          <Styled.Actions>
            {onRetry && (
              <Button onClick={onRetry} style={{ padding: '8px 18px', fontSize: 13 }}>
                Retry
              </Button>
            )}
            <Button variant="ghost" onClick={onDismiss} style={{ padding: '8px 14px', fontSize: 13 }}>
              Dismiss
            </Button>
          </Styled.Actions>
        </Styled.Body>
      </Styled.Content>
    </Styled.Overlay>
  );
}
