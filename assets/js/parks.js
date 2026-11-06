// Source unique de vérité pour les parcs : carte « Mes vidéos », page parc,
// règles de rattachement des vidéos (hashtags / mots-clés).

const SHARED_US_VIDEO = 'https://www.tiktok.com/@clip2ep.fan/video/7616407866504711446';

export const PARKS = [
  {
    key: 'europapark',
    name: 'Europa-Park',
    card: 'img/card/EP_card.webp',
    logo: 'img/logos/LOGO_ep.webp',
    map: 'Europa-Park Rust Germany',
    hashtags: ['#europapark'],
    keywords: ['europa-park', 'europapark']
  },
  {
    key: 'efteling',
    name: 'Efteling',
    card: 'img/card/EF_card.webp',
    logo: 'img/logos/LOGO_efteling.webp',
    map: 'Efteling Kaatsheuvel Netherlands',
    hashtags: ['#efteling'],
    keywords: ['efteling']
  },
  {
    key: 'bobbejaanland',
    aliases: ['bobejaanland'],
    name: 'Bobbejaanland',
    card: 'img/card/BOB_card.webp',
    logo: 'img/logos/LOGO_bob.webp',
    map: 'Bobbejaanland Lichtaart Belgium',
    hashtags: ['#bobbejaanland', '#bobejaanland'],
    keywords: ['bobbejaanland', 'bobejaanland']
  },
  {
    key: 'toverland',
    name: 'Toverland',
    card: 'img/card/toverland_card.webp',
    logo: 'img/logos/LOGO_toverland.webp',
    map: 'Toverland Sevenum Netherlands',
    hashtags: ['#toverland'],
    keywords: ['toverland']
  },
  {
    key: 'phantasialand',
    name: 'Phantasialand',
    card: 'img/card/PHANTA_card.webp',
    logo: 'img/logos/LOGO_phanta.webp',
    map: 'Phantasialand Bruehl Germany',
    hashtags: ['#phantasialand'],
    keywords: ['phantasialand']
  },
  {
    key: 'nigloland',
    name: 'Nigloland',
    card: 'img/card/NIGLO_card.webp',
    logo: 'img/logos/LOGO_niglo.webp',
    map: 'Nigloland Dolancourt France',
    hashtags: ['#nigloland', '#niglo'],
    keywords: ['nigloland', 'niglo']
  },
  {
    key: 'rulantica',
    name: 'Rulantica',
    card: 'img/card/Rulantica_card.webp',
    logo: 'img/logos/LOGO_rulantica.webp',
    map: 'Rulantica Rust Germany',
    hashtags: ['#rulantica'],
    keywords: ['rulantica', 'water park europa-park', 'parc aquatique europa-park', 'water world rulantica']
  },
  {
    key: 'energylandia',
    name: 'Energylandia',
    card: 'img/card/EN_card.webp',
    logo: 'img/logos/LOGO_en.webp',
    map: 'Energylandia Zator Poland',
    hashtags: ['#energylandia'],
    keywords: ['energylandia']
  },
  {
    key: 'parc-asterix',
    name: 'Parc Asterix',
    card: 'img/card/ASTERIX_card.webp',
    logo: 'img/logos/LOGO_asterix.webp',
    map: 'Parc Asterix Plailly France',
    hashtags: ['#parcasterix', '#parc-asterix'],
    keywords: ['parcasterix', 'parc-asterix', 'parc asterix']
  },
  {
    key: 'traumatica',
    name: 'Traumatica',
    card: 'img/card/TRAUMATICA_card.webp',
    logo: 'img/logos/LOGO_traumatica.webp',
    map: 'Traumatica Rust Germany',
    hashtags: ['#traumatica'],
    keywords: ['traumatica']
  },
  {
    key: 'seaworld',
    name: 'SeaWorld',
    card: 'img/card/SEAWORLD_card.webp',
    logo: 'img/logos/LOGO_seaworld.svg',
    map: 'SeaWorld Orlando Florida',
    hashtags: ['#seaworld', '#seaworldorlando'],
    keywords: ['seaworld', 'sea world', 'seaworld orlando'],
    extraVideos: [SHARED_US_VIDEO],
    usTag: true
  },
  {
    key: 'universal',
    name: 'Universal',
    card: 'img/card/UNIVERSAL_card.webp',
    logo: 'img/logos/LOGO_epic.webp',
    map: 'Universal Orlando Resort Florida',
    hashtags: ['#universalorlando', '#epicuniverse', '#islandsofadventure', '#islandofadventure', '#universalstudiosorlando'],
    keywords: ['universal orlando', 'universalorlando', 'epic universe', 'epicuniverse', 'islands of adventure', 'islandsofadventure'],
    extraVideos: [SHARED_US_VIDEO],
    usTag: true
  },
  {
    key: 'disneyworld',
    name: 'Disney World',
    card: 'img/card/DISNEYWORLD_card.webp',
    logo: 'img/logos/LOGO_disneyworld.webp',
    map: 'Walt Disney World Resort Orlando Florida',
    hashtags: ['#disneyworld', '#waltdisneyworld', '#disney', '#disneyland', '#wdw', '#magickingdom', '#epcot', '#animalkingdom', '#hollywoodstudios', '#disneyparks', '#waltdisney', '#disneytrip', '#disneyfan'],
    keywords: ['disney world', 'disneyworld', 'walt disney world', 'waltdisneyworld', 'magic kingdom', 'epcot', 'animal kingdom', 'hollywood studios', 'wdw', 'disneyland'],
    extraVideos: [SHARED_US_VIDEO]
  }
];

export function findPark(key) {
  const wanted = String(key || '').toLowerCase();
  return PARKS.find((park) => park.key === wanted || (park.aliases || []).includes(wanted)) || null;
}

// Tampons de la section Partenariats (ordre d'affichage conservé) ; `country` = drapeau affiché.
export const PARTNER_LOGOS = [
  { name: 'Toverland', logo: 'img/logos/LOGO_toverland.webp', url: 'https://www.toverland.com', country: 'nl' },
  { name: 'Efteling', logo: 'img/logos/LOGO_efteling.webp', url: 'https://www.efteling.com', country: 'nl' },
  { name: 'Bobbejaanland', logo: 'img/logos/LOGO_bob.webp', url: 'https://www.bobbejaanland.be', country: 'be' },
  { name: 'Energylandia', logo: 'img/logos/LOGO_en.webp', url: 'https://energylandia.pl', country: 'pl' }
];

export const FILMED_LOGOS = [
  { name: 'Europa-Park', logo: 'img/logos/LOGO_ep.webp', url: 'https://www.europapark.de', country: 'de' },
  { name: 'SeaWorld', logo: 'img/logos/LOGO_seaworld.svg', url: 'https://seaworld.com', country: 'us' },
  { name: 'Phantasialand', logo: 'img/logos/LOGO_phanta.webp', url: 'https://www.phantasialand.de', country: 'de' },
  { name: 'Nigloland', logo: 'img/logos/LOGO_niglo.webp', url: 'https://www.nigloland.fr', country: 'fr' },
  { name: 'Universal Epic Universe', logo: 'img/logos/LOGO_epic.webp', url: 'https://www.universalorlando.com/web/en/us/theme-parks/epic-universe', country: 'us' },
  { name: 'Islands of Adventure', logo: 'img/logos/LOGO_islandoadv.avif', url: 'https://www.universalorlando.com/web/en/us/theme-parks/islands-of-adventure', country: 'us' },
  { name: 'Disney World', logo: 'img/logos/LOGO_disneyworld.webp', url: 'https://disneyworld.disney.go.com', country: 'us' },
  { name: 'Rulantica', logo: 'img/logos/LOGO_rulantica.webp', url: 'https://www.rulantica.de', country: 'de' },
  { name: 'Parc Astérix', logo: 'img/logos/LOGO_asterix.webp', url: 'https://www.parcasterix.fr', country: 'fr' },
  { name: 'Traumatica', logo: 'img/logos/LOGO_traumatica.webp', url: 'https://www.traumatica.com', country: 'de' }
];
