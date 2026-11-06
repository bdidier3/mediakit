// Effets partagés : afficheur à palettes et déclenchement à l'apparition.

import { prefersReducedMotion } from './ui.js';

const DIGITS = '0123456789';
const LETTERS = 'ABCDEFGHJKMNPRSTUVWXYZ';

// Déclenche `onReveal(element)` une seule fois, quand l'élément devient visible.
export function revealOnView(elements, onReveal, threshold = 0.3) {
  const list = [...elements];
  if (!('IntersectionObserver' in window)) {
    list.forEach(onReveal);
    return;
  }
  const observer = new IntersectionObserver((entries) => {
    entries.forEach((entry) => {
      if (entry.isIntersecting) {
        observer.unobserve(entry.target);
        onReveal(entry.target);
      }
    });
  }, { threshold });
  list.forEach((element) => observer.observe(element));
}

// Remplace le texte par des palettes ; avec `animate`, chaque palette défile
// quelques caractères avant de s'arrêter sur le bon, de gauche à droite.
export function renderFlap(element, text, animate = false) {
  element.setAttribute('aria-label', text);
  element.innerHTML = '';
  const tiles = [...text].map((char) => {
    if (/\s/.test(char)) {
      const gap = document.createElement('span');
      gap.setAttribute('aria-hidden', 'true');
      gap.style.width = '0.25em';
      element.append(gap);
      return null;
    }
    const tile = document.createElement('span');
    tile.className = 'flap-char';
    tile.setAttribute('aria-hidden', 'true');
    tile.textContent = char;
    element.append(tile);
    return { tile, char };
  }).filter(Boolean);

  if (!animate || prefersReducedMotion()) {
    return;
  }

  tiles.forEach(({ tile, char }, index) => {
    const pool = DIGITS.includes(char) ? DIGITS : /[A-Z]/i.test(char) ? LETTERS : null;
    if (!pool) {
      return;
    }
    let flips = 6 + index * 3;
    const timer = setInterval(() => {
      flips -= 1;
      tile.classList.remove('is-flipping');
      void tile.offsetWidth;
      tile.classList.add('is-flipping');
      if (flips <= 0) {
        tile.textContent = char;
        clearInterval(timer);
        return;
      }
      tile.textContent = pool[Math.floor(Math.random() * pool.length)];
    }, 60);
  });
}
