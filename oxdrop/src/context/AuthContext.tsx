import { createContext, useState, useEffect, useCallback, type ReactNode } from 'react';
import type { IUser } from '@/types';
import * as commands from '@/lib/commands';

interface AuthState {
  isLoading: boolean;
  isAuthenticated: boolean;
  token: string | null;
  user: IUser | null;
  host: string | null;
  login: (identifier: string, password: string, host?: string) => Promise<void>;
  signIn: (token: string, host?: string) => Promise<void>;
  signOut: () => Promise<void>;
}

export const AuthContext = createContext<AuthState | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [isLoading, setIsLoading] = useState(true);
  const [token, setToken] = useState<string | null>(null);
  const [user, setUser] = useState<IUser | null>(null);
  const [host, setHost] = useState<string | null>(null);

  useEffect(() => {
    commands
      .checkAuth()
      .then((info) => {
        if (info) {
          setToken(info.token);
          setUser(info.user);
          setHost(info.host);
        }
      })
      .finally(() => setIsLoading(false));
  }, []);

  const login = useCallback(async (identifier: string, password: string, newHost?: string) => {
    const info = await commands.login(identifier, password, newHost);
    setToken(info.token);
    setUser(info.user);
    setHost(info.host);
  }, []);

  const signIn = useCallback(async (newToken: string, newHost?: string) => {
    await commands.saveAuthToken(newToken, newHost);
    setToken(newToken);
    setHost(newHost ?? null);
    // Re-check auth to get user info
    const info = await commands.checkAuth();
    if (info) {
      setUser(info.user);
      setHost(info.host);
    }
  }, []);

  const signOut = useCallback(async () => {
    await commands.clearAuthToken();
    setToken(null);
    setUser(null);
  }, []);

  return (
    <AuthContext.Provider
      value={{
        isLoading,
        isAuthenticated: !!token,
        token,
        user,
        host,
        login,
        signIn,
        signOut,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}
