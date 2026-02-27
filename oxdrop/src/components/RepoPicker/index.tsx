import { useState, useEffect, useRef } from 'react';
import { Button } from '@/ui/Button';
import { Input } from '@/ui/Input';
import * as commands from '@/lib/commands';
import type { IRepository } from '@/types';
import * as Styled from './styled';

interface RepoPickerProps {
  value: string | null;
  onChange: (repo: IRepository) => void;
}

export function RepoPicker({ value, onChange }: RepoPickerProps) {
  const [repos, setRepos] = useState<IRepository[]>([]);
  const [loading, setLoading] = useState(false);
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState('');
  const [showCreate, setShowCreate] = useState(false);
  const [newNamespace, setNewNamespace] = useState('');
  const [newName, setNewName] = useState('');
  const [creating, setCreating] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setLoading(true);
    commands
      .listRepos()
      .then(setRepos)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  // Auto-select if user has exactly one repo
  useEffect(() => {
    if (repos.length === 1 && !value) {
      onChange(repos[0]);
    }
  }, [repos, value, onChange]);

  // Close on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const filtered = repos.filter((r) => r.displayName.toLowerCase().includes(search.toLowerCase()));

  const handleCreate = async () => {
    if (!newNamespace.trim() || !newName.trim()) return;
    setCreating(true);
    try {
      const repo = await commands.createRepo(newNamespace.trim(), newName.trim());
      setRepos((prev) => [...prev, repo]);
      onChange(repo);
      setShowCreate(false);
      setOpen(false);
      setNewNamespace('');
      setNewName('');
    } catch {
      // TODO: show error
    } finally {
      setCreating(false);
    }
  };

  return (
    <Styled.Container ref={ref}>
      <Styled.Row>
        <div style={{ flex: 1 }}>
          <Styled.Trigger $open={open} onClick={() => setOpen(!open)}>
            <span>{value || (loading ? 'Loading...' : 'Select a project')}</span>
            <Styled.Arrow>▼</Styled.Arrow>
          </Styled.Trigger>
        </div>
        <Button variant="secondary" onClick={() => setShowCreate(true)} style={{ padding: '10px 16px', fontSize: 13 }}>
          + New
        </Button>
      </Styled.Row>

      {open && (
        <Styled.Dropdown>
          <Styled.SearchInput
            placeholder="Search projects..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            autoFocus
          />
          {filtered.length === 0 && <Styled.EmptyText>No projects found</Styled.EmptyText>}
          {filtered.map((repo) => (
            <Styled.Option
              key={repo.displayName}
              $selected={repo.displayName === value}
              onClick={() => {
                onChange(repo);
                setOpen(false);
                setSearch('');
              }}
            >
              {repo.displayName}
            </Styled.Option>
          ))}
        </Styled.Dropdown>
      )}

      {showCreate && (
        <Styled.Dropdown>
          <div style={{ padding: 12, display: 'flex', flexDirection: 'column', gap: 8 }}>
            <Input placeholder="Namespace (e.g. your-name)" value={newNamespace} onChange={(e) => setNewNamespace(e.target.value)} autoFocus />
            <Input placeholder="Project name" value={newName} onChange={(e) => setNewName(e.target.value)} />
            <div style={{ display: 'flex', gap: 8, marginTop: 4 }}>
              <Button onClick={handleCreate} disabled={creating} style={{ flex: 1, padding: '8px 16px', fontSize: 13 }}>
                {creating ? 'Creating...' : 'Create'}
              </Button>
              <Button variant="ghost" onClick={() => setShowCreate(false)} style={{ padding: '8px 12px', fontSize: 13 }}>
                Cancel
              </Button>
            </div>
          </div>
        </Styled.Dropdown>
      )}
    </Styled.Container>
  );
}
