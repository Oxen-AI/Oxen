export interface AppTheme {
  colors: {
    bg: string;
    bgCard: string;
    bgCardHover: string;
    bgInput: string;
    bgSurface: string;
    border: string;
    borderFocus: string;
    text: string;
    textSecondary: string;
    textMuted: string;
    accent: string;
    accentHover: string;
    accentSoft: string;
    success: string;
    successSoft: string;
    danger: string;
    dangerSoft: string;
    dropzoneBg: string;
  };
  shadows: {
    dropdown: string;
    frame: string;
  };
  gradients: {
    progress: string;
    cta: string;
    avatar: string;
  };
}

export const darkTheme: AppTheme = {
  colors: {
    bg: '#0D0F11',
    bgCard: '#161A1E',
    bgCardHover: '#1C2127',
    bgInput: '#1C2127',
    bgSurface: '#12151A',
    border: '#2A2F37',
    borderFocus: '#5B8DEF',
    text: '#E8EAED',
    textSecondary: '#8B929A',
    textMuted: '#5A6170',
    accent: '#5B8DEF',
    accentHover: '#4A7DE0',
    accentSoft: 'rgba(91, 141, 239, 0.12)',
    success: '#34D399',
    successSoft: 'rgba(52, 211, 153, 0.12)',
    danger: '#EF6B6B',
    dangerSoft: 'rgba(239, 107, 107, 0.10)',
    dropzoneBg: 'transparent',
  },
  shadows: {
    dropdown: '0 12px 40px rgba(0, 0, 0, 0.4)',
    frame: '0 32px 80px rgba(0, 0, 0, 0.6), 0 0 0 1px rgba(255, 255, 255, 0.06)',
  },
  gradients: {
    progress: 'linear-gradient(90deg, #5B8DEF, #9B6DEF)',
    cta: 'linear-gradient(135deg, #5B8DEF, #9B6DEF)',
    avatar: 'linear-gradient(135deg, #5B8DEF, #9B6DEF)',
  },
};

export const lightTheme: AppTheme = {
  colors: {
    bg: '#FFFFFF',
    bgCard: '#F5F6F8',
    bgCardHover: '#EDEEF1',
    bgInput: '#F5F6F8',
    bgSurface: '#FAFAFA',
    border: '#E0E2E7',
    borderFocus: '#4A7DE0',
    text: '#1A1D23',
    textSecondary: '#5F6775',
    textMuted: '#9099A6',
    accent: '#4A7DE0',
    accentHover: '#3B6DD0',
    accentSoft: 'rgba(74, 125, 224, 0.08)',
    success: '#22A06B',
    successSoft: 'rgba(34, 160, 107, 0.10)',
    danger: '#DE350B',
    dangerSoft: 'rgba(222, 53, 11, 0.07)',
    dropzoneBg: '#FAFBFC',
  },
  shadows: {
    dropdown: '0 12px 40px rgba(0, 0, 0, 0.04)',
    frame: '0 32px 80px rgba(0, 0, 0, 0.12), 0 0 0 1px rgba(0, 0, 0, 0.06)',
  },
  gradients: {
    progress: 'linear-gradient(90deg, #4A7DE0, #8B6DEF)',
    cta: 'linear-gradient(135deg, #4A7DE0, #8B6DEF)',
    avatar: 'linear-gradient(135deg, #4A7DE0, #8B6DEF)',
  },
};
