import { StyledInput, Label, LabelHint } from './styled';
import type { InputHTMLAttributes } from 'react';

interface InputProps extends InputHTMLAttributes<HTMLInputElement> {
  label?: string;
  hint?: string;
  mono?: boolean;
}

export function Input({ label, hint, mono, ...rest }: InputProps) {
  return (
    <div>
      {label && (
        <Label>
          {label} {hint && <LabelHint>{hint}</LabelHint>}
        </Label>
      )}
      <StyledInput $mono={mono} {...rest} />
    </div>
  );
}
