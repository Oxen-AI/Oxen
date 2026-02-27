import { getCurrentWindow } from '@tauri-apps/api/window';
import { useAuth } from '@/hooks/useAuth';
import { ArrowLeft } from 'lucide-react';
import * as Styled from './styled';

interface TitleBarProps {
  showBack?: boolean;
  onBack?: () => void;
}

export function TitleBar({ showBack, onBack }: TitleBarProps) {
  const { user, isAuthenticated } = useAuth();
  const appWindow = getCurrentWindow();

  return (
    <Styled.Bar data-tauri-drag-region>
      <Styled.Left>
        <Styled.TrafficLights>
          <Styled.TrafficLight $color="#FF5F57" onClick={() => appWindow.close()} />
          <Styled.TrafficLight $color="#FEBC2E" onClick={() => appWindow.minimize()} />
          <Styled.TrafficLight $color="#28C840" onClick={() => appWindow.toggleMaximize()} />
        </Styled.TrafficLights>
        {showBack && onBack && (
          <Styled.BackButton onClick={onBack}>
            <ArrowLeft size={14} />
            Back
          </Styled.BackButton>
        )}
      </Styled.Left>
      {isAuthenticated && user && (
        <Styled.UserDisplay>
          <Styled.Avatar>{user.name[0]?.toUpperCase() ?? '?'}</Styled.Avatar>
          <Styled.UserName>{user.email}</Styled.UserName>
        </Styled.UserDisplay>
      )}
    </Styled.Bar>
  );
}
