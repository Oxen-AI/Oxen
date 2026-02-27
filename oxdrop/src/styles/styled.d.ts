import 'styled-components';
import type { AppTheme } from './themes';

declare module 'styled-components' {
  export interface DefaultTheme extends AppTheme {}
}
