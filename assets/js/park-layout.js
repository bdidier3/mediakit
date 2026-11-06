// Disposition du parc, partagée par le plan 3D (accueil) et le plan 2D
// (pied de page, et secours si la 3D ne peut pas s'afficher).
// Coordonnées en mètres sur l'île : x vers la droite, z vers le visiteur.

export const ISLAND_RADIUS = 24;
export const LAKE_RADIUS = 6.2;
export const RING = { inner: 7.6, outer: 9.4 };

export const ZONE_LAYOUT = [
  { id: 'entrance', link: { href: 'https://www.tiktok.com/@clip2ep.fan', icon: 'tiktok', label: 'TikTok' }, pos: [0, 0, 18.5], color: '#ffc93c' },
  { id: 'shop', link: { href: 'https://www.youtube.com/@clip2ep-fan', icon: 'youtube', label: 'YouTube' }, pos: [-9.5, 0, 13], color: '#d7362b' },
  { id: 'about', station: 2, pos: [-7, 0, -10.5], color: '#ffc93c' },
  { id: 'stats', station: 3, pos: [-12.5, 0, 2], color: '#9bd3f2' },
  { id: 'partners', station: 4, pos: [6.5, 0, -11], color: '#f7f9fb' },
  { id: 'videos', station: 5, pos: [13, 0, 1.5], color: '#d7362b' },
  { id: 'contact', station: 6, pos: [6, 0, 12], color: '#9bd3f2' }
];
