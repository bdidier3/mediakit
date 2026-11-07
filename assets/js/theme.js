// Mode jour / nuit. Première visite : nuit. Ensuite, le dernier choix du
// visiteur est gardé. Le thème est posé sur <html data-theme> dès le <head>
// (petit script en ligne) pour éviter un flash au chargement.

import { onLanguageChange, t } from './i18n.js';

const STORAGE_KEY = 'siteTheme';
const listeners = new Set();

export function currentTheme() {
  return document.documentElement.dataset.theme === 'day' ? 'day' : 'night';
}

export function onThemeChange(callback) {
  listeners.add(callback);
}

function setTheme(theme) {
  document.documentElement.dataset.theme = theme;
  try {
    localStorage.setItem(STORAGE_KEY, theme);
  } catch {
    // Stockage indisponible : le choix vaut pour cette visite.
  }
  listeners.forEach((callback) => callback(theme));
}

export function initThemeToggle() {
  const button = document.querySelector('.theme-toggle');
  if (!button) {
    return;
  }
  // Le libellé annonce ce que fait le bouton : passer à l'autre mode.
  const relabel = () => {
    const label = t(currentTheme() === 'night' ? 'park.themeDay' : 'park.themeNight');
    button.setAttribute('aria-label', label);
    button.title = label;
  };
  button.addEventListener('click', () => {
    setTheme(currentTheme() === 'night' ? 'day' : 'night');
    relabel();
  });
  onLanguageChange(relabel);
  relabel();
}
