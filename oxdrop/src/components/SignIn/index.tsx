import { useState } from 'react';
import { useAuth } from '@/hooks/useAuth';
import { Button } from '@/ui/Button';
import { Input } from '@/ui/Input';
import * as Styled from './styled';

type Mode = 'welcome' | 'login' | 'token';

interface SignInProps {
  onSignedIn: () => void;
}

export function SignIn({ onSignedIn }: SignInProps) {
  const { login, signIn } = useAuth();
  const [mode, setMode] = useState<Mode>('welcome');

  // Login form state
  const [identifier, setIdentifier] = useState('');
  const [password, setPassword] = useState('');

  // Token form state
  const [token, setToken] = useState('');
  const [host, setHost] = useState('');

  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const handleLogin = async () => {
    if (!identifier.trim()) {
      setError('Please enter your email or username.');
      return;
    }
    if (!password) {
      setError('Please enter your password.');
      return;
    }
    setLoading(true);
    setError('');
    try {
      await login(identifier.trim(), password);
      onSignedIn();
    } catch (e: unknown) {
      const err = e as { code?: string; message?: string } | undefined;
      if (err?.code === 'BAD_CREDENTIALS') {
        setError('Invalid username or password.');
      } else if (err?.code === 'CONNECTION_ERROR') {
        setError('Could not connect to the server.');
      } else {
        setError(err?.message ?? 'Login failed. Please try again.');
      }
    } finally {
      setLoading(false);
    }
  };

  const handleTokenSubmit = async () => {
    if (!token.trim()) {
      setError('Please enter your API token.');
      return;
    }
    setLoading(true);
    setError('');
    try {
      await signIn(token.trim(), host.trim() || undefined);
      onSignedIn();
    } catch {
      setError('Failed to save token. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  if (mode === 'welcome') {
    return (
      <Styled.Container>
        <Styled.Logo>🐂</Styled.Logo>
        <Styled.Title>Welcome to OxDrop</Styled.Title>
        <Styled.Subtitle>
          Upload files and folders to your projects — no command line required.
        </Styled.Subtitle>
        <Styled.SignInButton onClick={() => setMode('login')}>Sign in to Oxen</Styled.SignInButton>
        <Styled.Hint>Sign in with your OxenHub account</Styled.Hint>
      </Styled.Container>
    );
  }

  if (mode === 'login') {
    return (
      <Styled.Container>
        <Styled.Logo>🐂</Styled.Logo>
        <Styled.Title>Sign in</Styled.Title>
        <Styled.Subtitle>
          Enter your OxenHub email and password.
        </Styled.Subtitle>
        <Styled.TokenForm>
          <Input
            placeholder="Email or username"
            value={identifier}
            onChange={(e) => setIdentifier(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleLogin()}
            autoFocus
          />
          <Input
            type="password"
            placeholder="Password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleLogin()}
          />
          {error && <Styled.ErrorText>{error}</Styled.ErrorText>}
          <Button onClick={handleLogin} disabled={loading} fullWidth>
            {loading ? 'Signing in...' : 'Sign in'}
          </Button>
          <Styled.ToggleLink onClick={() => { setError(''); setMode('token'); }}>
            Use API token instead
          </Styled.ToggleLink>
          <Button variant="ghost" onClick={() => setMode('welcome')}>
            Back
          </Button>
        </Styled.TokenForm>
      </Styled.Container>
    );
  }

  // mode === 'token'
  return (
    <Styled.Container>
      <Styled.Logo>🐂</Styled.Logo>
      <Styled.Title>Sign in with API token</Styled.Title>
      <Styled.Subtitle>
        Paste your API token from OxenHub. You can find it in your account settings.
      </Styled.Subtitle>
      <Styled.TokenForm>
        <Input
          placeholder="Paste your API token"
          value={token}
          onChange={(e) => setToken(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleTokenSubmit()}
          autoFocus
        />
        <Input
          placeholder="Host (default: hub.oxen.ai)"
          value={host}
          onChange={(e) => setHost(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleTokenSubmit()}
        />
        {error && <Styled.ErrorText>{error}</Styled.ErrorText>}
        <Button onClick={handleTokenSubmit} disabled={loading} fullWidth>
          {loading ? 'Signing in...' : 'Sign in'}
        </Button>
        <Styled.ToggleLink onClick={() => { setError(''); setMode('login'); }}>
          Sign in with password instead
        </Styled.ToggleLink>
        <Button variant="ghost" onClick={() => setMode('welcome')}>
          Back
        </Button>
      </Styled.TokenForm>
    </Styled.Container>
  );
}
