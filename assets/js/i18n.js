// Toutes les traductions du site (accueil + page parc) et la logique de langue.

export const LANGUAGES = {
  fr: { label: 'Français', flag: 'fr', locale: 'fr-FR' },
  en: { label: 'English', flag: 'gb', locale: 'en-US' },
  es: { label: 'Español', flag: 'es', locale: 'es-ES' },
  de: { label: 'Deutsch', flag: 'de', locale: 'de-DE' },
  nl: { label: 'Nederlands', flag: 'nl', locale: 'nl-NL' },
  pl: { label: 'Polski', flag: 'pl', locale: 'pl-PL' },
  it: { label: 'Italiano', flag: 'it', locale: 'it-IT' },
  sv: { label: 'Svenska', flag: 'se', locale: 'sv-SE' },
  ar: { label: 'العربية', flag: 'sa', locale: 'ar-u-nu-latn' },
  zh: { label: '中文', flag: 'cn', locale: 'zh-CN' },
  ja: { label: '日本語', flag: 'jp', locale: 'ja-JP' }
};

const DEFAULT_LANGUAGE = 'en';
const STORAGE_KEY = 'siteLanguage';

export const translations = {
  fr: {
    titles: { home: 'Clip2EP', videos: 'Vidéos parc | Clip2EP' },
    meta: { home: "Site officiel de Clip2EP - créateur de contenu sur les parcs d'attractions sur TikTok et YouTube." },
    nav: { label: 'Navigation principale', open: 'Ouvrir le menu', language: 'Choisir la langue', home: 'Accueil', stats: 'Statistiques', about: 'À propos', partners: 'Partenariats', videos: 'Mes vidéos', contact: 'Contact' },
    common: { footerMain: '© Clip2EP - 2026', skip: 'Aller au contenu' },
    park: { station: 'Station', welcome: 'Bienvenue dans le parc', welcomeAfter: '', editorial: 'Ma ligne éditoriale', badgeRole: 'Créateur de contenu', stampPartner: 'Partenaire officiel', stampVisited: 'Visité', ticket: 'Billet n°', gearSign: 'Équipement autorisé à bord', booth: 'Point info', exit: 'Sortie', bye: 'À bientôt\u00a0!', map: 'Plan du parc', here: 'Vous êtes ici', zone: { entrance: "L'Entrée", shop: 'La Boutique', stats: 'La Tour des records', about: 'La Grande Roue', partners: 'Le Château des partenaires', videos: 'Le Grand Huit', contact: 'Le Kiosque info' }, mapHint: 'Clique sur une attraction pour la visiter. Glisse pour faire tourner et incliner le parc.', mapAria: 'Plan 3D interactif du parc Clip2EP', rotateLeft: 'Tourner le parc à gauche', rotateRight: 'Tourner le parc à droite', screen: "En direct de l'attraction", footerThanks: 'Merci pour la visite du parc Clip2EP ! Pour une collaboration, le point info est ouvert toute l\'année.', stationsTitle: 'Les stations', follow: 'Suivre Clip2EP', backTop: 'Retour à l\'entrée', loading: 'Ouverture du parc…' },
    home: {
      videoAria: "Vidéo de fond d'un parc d'attractions",
      title: "L'un des plus gros créateurs de contenu français dédiés aux parcs d'attractions",
      lead: "+150 000 abonnés sur TikTok et +50 000 sur YouTube, contenus à fort engagement autour des plus grands parcs d'attractions du monde !"
    },
    stats: {
      heading: "Statistiques d'audience",
      subheading: 'Aperçu mis à jour de vos derniers insights.',
      gender: 'Répartition par genre',
      age: "Tranches d'âge",
      regions: 'Top régions',
      age65: '65 ans et plus',
      genderLabels: { men: 'Homme', women: 'Femme', other: 'Autre', userSpecified: "Spécifié par l'utilisateur" },
      region: { france: 'France', germany: 'Allemagne', belgium: 'Belgique', switzerland: 'Suisse', usa: 'États-Unis', canada: 'Canada', reunion: 'La Réunion' },
      tiktok: { since: 'Depuis 2023', kpiViews: 'Vues par an', kpiFollowers: 'Abonnés' },
      youtube: { since: 'Depuis le 25 sept. 2025', kpiViews: 'Vues totales', kpiFollowers: 'Abonnés' }
    },
    aboutme: {
      kicker: 'À propos de moi',
      title: "Créateur de contenu parcs d'attractions",
      p1: "Je m'appelle <strong>Baptiste DIDIER</strong>, j'ai 19 ans. Je fais actuellement des études en informatique à Belfort, et je consacre mon temps libre à la création de contenu spécialisé dans les <strong>parcs d'attractions</strong>. Je publie des vidéos courtes chaque jour, dynamiques et engageantes, avec un angle éditorial orienté conseils pratiques et souvenirs de visite.",
      p2: "Ma communauté la plus forte est sur <strong>TikTok</strong>, avec un contenu d'abord centré sur <strong>Europa-Park</strong> puis élargi à de grandes destinations européennes. J'ai déjà réalisé des contenus dans plusieurs parcs majeurs, avec une audience réactive et fidèle.",
      p3: "Mon objectif est simple : produire des contenus <strong>modernes, authentiques et performants</strong>, en suivant les tendances du moment, afin de valoriser votre parc auprès d'un public français passionné.",
      photoAlt: "Baptiste dans un parc d'attractions"
    },
    partners: { title: 'Parcs en partenariat officiel', subtitle: 'Collaborations confirmées avec des parcs européens reconnus.' },
    nonPartners: { title: 'Parcs visités sans partenariat', subtitle: 'Mis en avant dans mes vidéos sans partenariat officiel.' },
    cases: { viewsLabel: 'vues au total', previous: 'Précédent', next: 'Suivant' },
    myVideos: {
      title: 'Mes vidéos',
      p1: "Je publie en moyenne <strong>une vidéo par jour</strong>, consacrée à un parc ou à une attraction, en m'appuyant sur les tendances du moment sur les réseaux sociaux. Mes formats incluent notamment des <strong>POV d'attractions</strong>, des contenus réaction et des séquences immersives pensées pour capter l'attention dès les premières secondes.",
      p2: "Ma ligne éditoriale repose sur des accroches qui favorisent l'interaction : souvenirs liés aux parcs, identification d'amis pour une prochaine visite, observations sur les comportements des visiteurs, astuces pratiques et comparatifs d'attractions entre différents parcs.",
      gearTitle: 'Mon matériel vidéo',
      gearNote: "Je réalise l'ensemble de mes contenus avec cet équipement, qui constitue mon matériel principal de tournage au quotidien."
    },
    gear: { harness: 'Harnais torse', pole: 'Perche' },
    contact: {
      title: 'Contact',
      subtitle: 'Pour les partenariats vous pouvez me contacter via ces différentes adresses',
      phoneLabel: 'Téléphone',
      reachOut: 'Vous pouvez me contacter via TikTok, mail, numéro et Instagram.'
    },
    videos: {
      back: "Retour à l'accueil", parkTitle: 'Parc', parkLead: 'Toutes les vidéos et performances disponibles pour ce parc.',
      viewBoth: 'TikTok + YouTube', viewTikTok: 'TikTok', viewYouTube: 'YouTube', platformFilter: 'Plateforme',
      layoutLabel: 'Affichage', layoutStack: 'Liste', layoutDuo: 'Côte à côte', layoutCompact: 'Compact',
      totalsTitle: 'Stats sur le parc', totalVideos: 'vidéos', totalViews: 'vues', totalLikes: 'likes', totalComments: 'commentaires',
      noVideos: 'Aucune vidéo enregistrée pour le moment.', noHashtag: 'Aucun hashtag détecté', linkUnavailable: 'Lien indisponible',
      loading: 'Chargement des statistiques...', loadError: 'Impossible de charger les statistiques. Rechargez la page pour réessayer.',
      noParkSelected: 'Aucun parc sélectionné.', comingSoon: 'Les détails vidéos pour ce parc arrivent bientôt.',
      mapCta: 'Voir sur Google Maps', mapTitle: 'Carte du parc', thumbAlt: 'Miniature vidéo',
      viewsUnit: 'vues', likesUnit: 'likes', commentsUnit: 'commentaires'
    }
  },
  en: {
    titles: { home: 'Clip2EP', videos: 'Park videos | Clip2EP' },
    meta: { home: 'Official Clip2EP site - theme park content creator on TikTok and YouTube.' },
    nav: { label: 'Main navigation', open: 'Open menu', language: 'Choose language', home: 'Home', stats: 'Analytics', about: 'About', partners: 'Partnerships', videos: 'My Videos', contact: 'Contact' },
    common: { footerMain: '© Clip2EP - 2026', skip: 'Skip to content' },
    park: { station: 'Station', welcome: 'Welcome to the', welcomeAfter: ' park', editorial: 'My editorial line', badgeRole: 'Content creator', stampPartner: 'Official partner', stampVisited: 'Visited', ticket: 'Ticket no.', gearSign: 'Equipment allowed on board', booth: 'Guest info', exit: 'Exit', bye: 'See you soon!', map: 'Park map', here: 'You are here', zone: { entrance: 'The Entrance', shop: 'The Shop', stats: 'Record Tower', about: 'The Big Wheel', partners: "Partners' Castle", videos: 'The Coaster', contact: 'Info Kiosk' }, mapHint: 'Click a ride to visit it. Drag to turn and tilt the park.', mapAria: 'Interactive 3D map of the Clip2EP park', rotateLeft: 'Turn the park left', rotateRight: 'Turn the park right', screen: 'Live from the ride', footerThanks: 'Thanks for visiting the Clip2EP park! For a collaboration, the info point is open all year round.', stationsTitle: 'Stations', follow: 'Follow Clip2EP', backTop: 'Back to the entrance', loading: 'Opening the park…' },
    home: {
      videoAria: 'Theme park background video',
      title: 'One of the largest French theme park content creators',
      lead: "+150,000 followers on TikTok and +50,000 on YouTube, high-engagement content around the world's biggest theme parks!"
    },
    stats: {
      heading: 'Audience Analytics',
      subheading: 'Updated snapshot from your latest platform insights.',
      gender: 'Gender split',
      age: 'Age groups',
      regions: 'Top regions',
      age65: '65 and above',
      genderLabels: { men: 'Men', women: 'Women', other: 'Other', userSpecified: 'User specified' },
      region: { france: 'France', germany: 'Germany', belgium: 'Belgium', switzerland: 'Switzerland', usa: 'United States', canada: 'Canada', reunion: 'La Réunion' },
      tiktok: { since: 'Since 2023', kpiViews: 'Views per year', kpiFollowers: 'Followers' },
      youtube: { since: 'Since Sep 25, 2025', kpiViews: 'Total views', kpiFollowers: 'Subscribers' }
    },
    aboutme: {
      kicker: 'About Me',
      title: 'From French theme park content to European collaborations.',
      p1: 'I am Baptiste, a French creator focused on theme parks with a highly engaged audience. I create short, energetic and story-driven videos designed to make people want to visit parks in real life.',
      p2: 'My strongest community is on TikTok, with content initially centered on Europa-Park and now expanded to major European destinations. I have already produced videos in parks such as Energylandia, Efteling, Toverland and Universal Orlando.',
      p3: 'My goal is simple: deliver modern, authentic, high-performing content that highlights your park experience for a passionate French audience.',
      photoAlt: 'Baptiste in a theme park'
    },
    partners: { title: 'Official Park Partnerships', subtitle: 'Trusted collaborations with leading European parks.' },
    nonPartners: { title: 'Parks Where I Filmed (No Partnership)', subtitle: 'Featured in my videos without official partnership.' },
    cases: { viewsLabel: 'total views', previous: 'Previous', next: 'Next' },
    myVideos: {
      title: 'My Videos',
      p1: 'I publish around one video per day, focused on a park or attraction, by leveraging current social media trends. My formats include ride POVs, reaction content, and immersive sequences designed to capture attention in the first seconds.',
      p2: 'My editorial style relies on hooks that drive interaction: park memories, tagging friends for future visits, observations about guest behavior, practical tips, and attraction comparisons across different parks.',
      gearTitle: 'My filming gear',
      gearNote: 'I create all my content with this equipment, which is my main filming setup on a daily basis.'
    },
    gear: { harness: 'Chest harness', pole: 'Selfie pole' },
    contact: {
      title: 'Contact',
      subtitle: 'For partnerships, you can contact me through these different addresses.',
      phoneLabel: 'Phone',
      reachOut: 'You can contact me via TikTok, email, phone number, and Instagram.'
    },
    videos: {
      back: 'Back to home', parkTitle: 'Park', parkLead: 'All available videos and performance for this park.',
      viewBoth: 'TikTok + YouTube', viewTikTok: 'TikTok', viewYouTube: 'YouTube', platformFilter: 'Platform',
      layoutLabel: 'Layout', layoutStack: 'List', layoutDuo: 'Side by side', layoutCompact: 'Compact',
      totalsTitle: 'Total stats', totalVideos: 'videos', totalViews: 'views', totalLikes: 'likes', totalComments: 'comments',
      noVideos: 'No videos available yet.', noHashtag: 'No hashtag detected', linkUnavailable: 'Link unavailable',
      loading: 'Loading statistics...', loadError: 'Statistics could not be loaded. Reload the page to try again.',
      noParkSelected: 'No park selected.', comingSoon: 'Video details for this park are coming soon.',
      mapCta: 'Open in Google Maps', mapTitle: 'Park map', thumbAlt: 'Video thumbnail',
      viewsUnit: 'views', likesUnit: 'likes', commentsUnit: 'comments'
    }
  },
  es: {
    titles: { home: 'Clip2EP', videos: 'Videos del parque | Clip2EP' },
    meta: { home: 'Sitio oficial de Clip2EP - creador de contenido de parques temáticos en TikTok y YouTube.' },
    nav: { label: 'Navegación principal', open: 'Abrir el menú', language: 'Elegir idioma', home: 'Inicio', stats: 'Estadísticas', about: 'Acerca de', partners: 'Colaboraciones', videos: 'Mis videos', contact: 'Contacto' },
    common: { footerMain: '© Clip2EP - 2026', skip: 'Ir al contenido' },
    park: { station: 'Estación', welcome: 'Bienvenido al parque', welcomeAfter: '', editorial: 'Mi línea editorial', badgeRole: 'Creador de contenido', stampPartner: 'Socio oficial', stampVisited: 'Visitado', ticket: 'Billete n.º', gearSign: 'Equipo permitido a bordo', booth: 'Punto de información', exit: 'Salida', bye: '¡Hasta pronto!', map: 'Mapa del parque', here: 'Estás aquí', zone: { entrance: 'La Entrada', shop: 'La Tienda', stats: 'La Torre de récords', about: 'La Noria', partners: 'El Castillo de socios', videos: 'La Montaña rusa', contact: 'El Quiosco de información' }, mapHint: 'Haz clic en una atracción para visitarla. Arrastra para girar e inclinar el parque.', mapAria: 'Mapa 3D interactivo del parque Clip2EP', rotateLeft: 'Girar el parque a la izquierda', rotateRight: 'Girar el parque a la derecha', screen: 'En directo desde la atracción', footerThanks: '¡Gracias por visitar el parque Clip2EP! Para una colaboración, el punto de información está abierto todo el año.', stationsTitle: 'Las estaciones', follow: 'Seguir a Clip2EP', backTop: 'Volver a la entrada', loading: 'Abriendo el parque…' },
    home: {
      videoAria: 'Video de fondo de parque temático',
      title: 'Uno de los mayores creadores franceses de contenido sobre parques temáticos',
      lead: '+150 000 seguidores en TikTok y +50 000 en YouTube, contenido de alto engagement sobre los parques temáticos más grandes del mundo.'
    },
    stats: {
      heading: 'Análisis de audiencia',
      subheading: 'Resumen actualizado de tus últimas métricas de plataforma.',
      gender: 'Distribución por género',
      age: 'Grupos de edad',
      regions: 'Regiones principales',
      age65: '65 años o más',
      genderLabels: { men: 'Hombres', women: 'Mujeres', other: 'Otro', userSpecified: 'Especificado por el usuario' },
      region: { france: 'Francia', germany: 'Alemania', belgium: 'Bélgica', switzerland: 'Suiza', usa: 'Estados Unidos', canada: 'Canadá', reunion: 'La Reunión' },
      tiktok: { since: 'Desde 2023', kpiViews: 'Visualizaciones por año', kpiFollowers: 'Seguidores' },
      youtube: { since: 'Desde el 25 sep. 2025', kpiViews: 'Visualizaciones totales', kpiFollowers: 'Suscriptores' }
    },
    aboutme: {
      kicker: 'Sobre mí',
      title: 'De contenido sobre parques temáticos en francés a colaboraciones europeas.',
      p1: 'Soy Baptiste, creador francés especializado en parques temáticos con una comunidad muy activa. Produzco videos cortos, dinámicos y narrativos para animar a la gente a visitar los parques en la vida real.',
      p2: 'Mi comunidad más fuerte está en TikTok, con contenido inicialmente centrado en Europa-Park y ahora ampliado a grandes destinos europeos. Ya he producido contenido en parques como Energylandia, Efteling, Toverland y Universal Orlando.',
      p3: 'Mi objetivo es claro: crear contenido moderno, auténtico y de alto rendimiento para destacar la experiencia de tu parque ante una audiencia francesa apasionada.',
      photoAlt: 'Baptiste en un parque temático'
    },
    partners: { title: 'Parques con colaboración oficial', subtitle: 'Colaboraciones confirmadas con parques europeos reconocidos.' },
    nonPartners: { title: 'Parques visitados sin colaboración', subtitle: 'Destacados en mis videos sin colaboración oficial.' },
    cases: { viewsLabel: 'visualizaciones totales', previous: 'Anterior', next: 'Siguiente' },
    myVideos: {
      title: 'Mis videos',
      p1: 'Publico de media un video al día, centrado en un parque o una atracción, aprovechando las tendencias actuales de redes sociales. Mis formatos incluyen POV de atracciones, contenido de reacción y secuencias inmersivas diseñadas para captar la atención en los primeros segundos.',
      p2: 'Mi línea editorial se basa en ganchos que fomentan la interacción: recuerdos en parques, etiquetar amigos para una próxima visita, observaciones sobre el comportamiento de los visitantes, consejos prácticos y comparaciones de atracciones entre distintos parques.',
      gearTitle: 'Mi equipo de grabación',
      gearNote: 'Creo todo mi contenido con este equipo, que es mi configuración principal de grabación diaria.'
    },
    gear: { harness: 'Arnés de pecho', pole: 'Palo selfie' },
    contact: {
      title: 'Contacto',
      subtitle: 'Para colaboraciones, puedes contactarme a través de estas diferentes direcciones.',
      phoneLabel: 'Teléfono',
      reachOut: 'Puedes contactarme por TikTok, email, teléfono e Instagram.'
    },
    videos: {
      back: 'Volver al inicio', parkTitle: 'Parque', parkLead: 'Todos los videos y resultados disponibles para este parque.',
      viewBoth: 'TikTok + YouTube', viewTikTok: 'TikTok', viewYouTube: 'YouTube', platformFilter: 'Plataforma',
      layoutLabel: 'Vista', layoutStack: 'Lista', layoutDuo: 'Lado a lado', layoutCompact: 'Compacto',
      totalsTitle: 'Estadísticas totales', totalVideos: 'videos', totalViews: 'vistas', totalLikes: 'me gusta', totalComments: 'comentarios',
      noVideos: 'No hay videos disponibles por ahora.', noHashtag: 'No se detectó hashtag', linkUnavailable: 'Enlace no disponible',
      loading: 'Cargando estadísticas...', loadError: 'No se pudieron cargar las estadísticas. Recarga la página para intentarlo de nuevo.',
      noParkSelected: 'Ningún parque seleccionado.', comingSoon: 'Los detalles de videos para este parque llegarán pronto.',
      mapCta: 'Abrir en Google Maps', mapTitle: 'Mapa del parque', thumbAlt: 'Miniatura del video',
      viewsUnit: 'vistas', likesUnit: 'me gusta', commentsUnit: 'comentarios'
    }
  },
  de: {
    titles: { home: 'Clip2EP', videos: 'Park-Videos | Clip2EP' },
    meta: { home: 'Offizielle Clip2EP-Seite - Freizeitpark-Content auf TikTok und YouTube.' },
    nav: { label: 'Hauptnavigation', open: 'Menü öffnen', language: 'Sprache wählen', home: 'Start', stats: 'Statistiken', about: 'Über mich', partners: 'Partnerschaften', videos: 'Meine Videos', contact: 'Kontakt' },
    common: { footerMain: '© Clip2EP - 2026', skip: 'Zum Inhalt springen' },
    park: { station: 'Station', welcome: 'Willkommen im', welcomeAfter: '-Park', editorial: 'Meine redaktionelle Linie', badgeRole: 'Content Creator', stampPartner: 'Offizieller Partner', stampVisited: 'Besucht', ticket: 'Ticket Nr.', gearSign: 'An Bord erlaubte Ausrüstung', booth: 'Info-Point', exit: 'Ausgang', bye: 'Bis bald!', map: 'Parkplan', here: 'Sie sind hier', zone: { entrance: 'Eingang', shop: 'Shop', stats: 'Rekordturm', about: 'Riesenrad', partners: 'Partnerschloss', videos: 'Achterbahn', contact: 'Info-Kiosk' }, mapHint: 'Klicke auf eine Attraktion, um sie zu besuchen. Ziehe, um den Park zu drehen und zu neigen.', mapAria: 'Interaktiver 3D-Plan des Clip2EP-Parks', rotateLeft: 'Park nach links drehen', rotateRight: 'Park nach rechts drehen', screen: 'Live aus dem Fahrgeschäft', footerThanks: 'Danke für deinen Besuch im Clip2EP-Park! Für eine Zusammenarbeit ist der Info-Point das ganze Jahr geöffnet.', stationsTitle: 'Die Stationen', follow: 'Clip2EP folgen', backTop: 'Zurück zum Eingang', loading: 'Der Park öffnet…' },
    home: {
      videoAria: 'Freizeitpark-Hintergrundvideo',
      title: 'Einer der größten französischen Content Creator für Freizeitparks',
      lead: '+150.000 Follower auf TikTok und +50.000 auf YouTube, stark performender Content rund um die größten Freizeitparks der Welt!'
    },
    stats: {
      heading: 'Audience-Analytik',
      subheading: 'Aktualisierte Übersicht deiner neuesten Plattform-Insights.',
      gender: 'Geschlechterverteilung',
      age: 'Altersgruppen',
      regions: 'Top-Regionen',
      age65: '65 und älter',
      genderLabels: { men: 'Männer', women: 'Frauen', other: 'Andere', userSpecified: 'Vom Nutzer angegeben' },
      region: { france: 'Frankreich', germany: 'Deutschland', belgium: 'Belgien', switzerland: 'Schweiz', usa: 'Vereinigte Staaten', canada: 'Kanada', reunion: 'La Réunion' },
      tiktok: { since: 'Seit 2023', kpiViews: 'Aufrufe pro Jahr', kpiFollowers: 'Follower' },
      youtube: { since: 'Seit 25. Sept. 2025', kpiViews: 'Gesamtaufrufe', kpiFollowers: 'Abonnenten' }
    },
    aboutme: {
      kicker: 'Über mich',
      title: 'Von französischem Freizeitpark-Content zu europäischen Kooperationen.',
      p1: 'Ich bin Baptiste, ein französischer Creator mit Fokus auf Freizeitparks und einer sehr engagierten Community. Ich produziere kurze, dynamische und erzählerische Videos, die Lust auf echte Parkerlebnisse machen.',
      p2: 'Meine stärkste Community ist auf TikTok. Der Content war zuerst auf Europa-Park ausgerichtet und wurde dann auf große europäische Destinationen erweitert. Ich habe bereits Inhalte in Energylandia, Efteling, Toverland und Universal Orlando produziert.',
      p3: 'Mein Ziel ist klar: moderne, authentische und leistungsstarke Inhalte liefern, die Ihr Parkerlebnis für ein begeistertes französisches Publikum hervorheben.',
      photoAlt: 'Baptiste in einem Freizeitpark'
    },
    partners: { title: 'Offizielle Park-Partnerschaften', subtitle: 'Vertrauensvolle Kooperationen mit führenden europäischen Parks.' },
    nonPartners: { title: 'Parks, in denen ich gedreht habe (ohne Partnerschaft)', subtitle: 'In meinen Videos gezeigt, ohne offizielle Partnerschaft.' },
    cases: { viewsLabel: 'Aufrufe gesamt', previous: 'Zurück', next: 'Weiter' },
    myVideos: {
      title: 'Meine Videos',
      p1: 'Ich veröffentliche durchschnittlich ein Video pro Tag, mit Fokus auf einen Park oder eine Attraktion, basierend auf aktuellen Social-Media-Trends. Meine Formate umfassen Ride-POVs, Reaktions-Content und immersive Sequenzen, die in den ersten Sekunden Aufmerksamkeit erzeugen.',
      p2: 'Mein redaktioneller Stil basiert auf Hooks, die Interaktion fördern: Park-Erinnerungen, Markierungen von Freunden für den nächsten Besuch, Beobachtungen zum Besucherverhalten, praktische Tipps und Vergleiche zwischen Attraktionen in verschiedenen Parks.',
      gearTitle: 'Meine Drehausrüstung',
      gearNote: 'Ich produziere all meine Inhalte mit diesem Equipment, das mein zentrales tägliches Dreh-Setup ist.'
    },
    gear: { harness: 'Brustgurt', pole: 'Selfie-Stange' },
    contact: {
      title: 'Kontakt',
      subtitle: 'Für Partnerschaften kannst du mich über diese verschiedenen Adressen kontaktieren.',
      phoneLabel: 'Telefon',
      reachOut: 'Du kannst mich über TikTok, E-Mail, Telefonnummer und Instagram kontaktieren.'
    },
    videos: {
      back: 'Zurück zur Startseite', parkTitle: 'Park', parkLead: 'Alle verfügbaren Videos und Leistungen für diesen Park.',
      viewBoth: 'TikTok + YouTube', viewTikTok: 'TikTok', viewYouTube: 'YouTube', platformFilter: 'Plattform',
      layoutLabel: 'Ansicht', layoutStack: 'Liste', layoutDuo: 'Nebeneinander', layoutCompact: 'Kompakt',
      totalsTitle: 'Gesamtstatistiken', totalVideos: 'Videos', totalViews: 'Aufrufe', totalLikes: 'Likes', totalComments: 'Kommentare',
      noVideos: 'Derzeit keine Videos verfügbar.', noHashtag: 'Kein Hashtag erkannt', linkUnavailable: 'Link nicht verfügbar',
      loading: 'Statistiken werden geladen...', loadError: 'Die Statistiken konnten nicht geladen werden. Lade die Seite neu, um es erneut zu versuchen.',
      noParkSelected: 'Kein Park ausgewählt.', comingSoon: 'Video-Details für diesen Park kommen bald.',
      mapCta: 'In Google Maps öffnen', mapTitle: 'Parkkarte', thumbAlt: 'Video-Vorschaubild',
      viewsUnit: 'Aufrufe', likesUnit: 'Likes', commentsUnit: 'Kommentare'
    }
  },
  nl: {
    titles: { home: 'Clip2EP', videos: "Parkvideo's | Clip2EP" },
    meta: { home: 'Officiële Clip2EP-site - pretparkcontent creator op TikTok en YouTube.' },
    nav: { label: 'Hoofdnavigatie', open: 'Menu openen', language: 'Taal kiezen', home: 'Home', stats: 'Statistieken', about: 'Over mij', partners: 'Partnerschappen', videos: "Mijn video's", contact: 'Contact' },
    common: { footerMain: '© Clip2EP - 2026', skip: 'Naar de inhoud' },
    park: { station: 'Station', welcome: 'Welkom in het', welcomeAfter: '-park', editorial: 'Mijn redactionele lijn', badgeRole: 'Contentcreator', stampPartner: 'Officiële partner', stampVisited: 'Bezocht', ticket: 'Ticket nr.', gearSign: 'Toegestane uitrusting aan boord', booth: 'Infopunt', exit: 'Uitgang', bye: 'Tot snel!', map: 'Plattegrond', here: 'U bent hier', zone: { entrance: 'De Ingang', shop: 'De Winkel', stats: 'Recordtoren', about: 'Reuzenrad', partners: 'Partnerkasteel', videos: 'Achtbaan', contact: 'Infokiosk' }, mapHint: 'Klik op een attractie om ze te bezoeken. Sleep om het park te draaien en te kantelen.', mapAria: 'Interactieve 3D-plattegrond van het Clip2EP-park', rotateLeft: 'Park naar links draaien', rotateRight: 'Park naar rechts draaien', screen: 'Live vanuit de attractie', footerThanks: 'Bedankt voor je bezoek aan het Clip2EP-park! Voor een samenwerking is het infopunt het hele jaar open.', stationsTitle: 'De stations', follow: 'Volg Clip2EP', backTop: 'Terug naar de ingang', loading: 'Het park gaat open…' },
    home: {
      videoAria: 'Pretpark achtergrondvideo',
      title: 'Een van de grootste Franse content creators voor pretparken',
      lead: '+150.000 volgers op TikTok en +50.000 op YouTube, sterk presterende content rond de grootste pretparken ter wereld!'
    },
    stats: {
      heading: 'Doelgroep-analytics',
      subheading: 'Bijgewerkte snapshot van je nieuwste platform-inzichten.',
      gender: 'Verdeling op geslacht',
      age: 'Leeftijdsgroepen',
      regions: "Topregio's",
      age65: '65 jaar en ouder',
      genderLabels: { men: 'Man', women: 'Vrouw', other: 'Overig', userSpecified: 'Door gebruiker opgegeven' },
      region: { france: 'Frankrijk', germany: 'Duitsland', belgium: 'België', switzerland: 'Zwitserland', usa: 'Verenigde Staten', canada: 'Canada', reunion: 'La Réunion' },
      tiktok: { since: 'Sinds 2023', kpiViews: 'Views per jaar', kpiFollowers: 'Volgers' },
      youtube: { since: 'Sinds 25 sep 2025', kpiViews: 'Totale views', kpiFollowers: 'Abonnees' }
    },
    aboutme: {
      kicker: 'Over mij',
      title: 'Van Franse pretparkcontent naar Europese samenwerkingen.',
      p1: "Ik ben Baptiste, een Franse creator gespecialiseerd in pretparken met een zeer betrokken community. Ik maak korte, energieke en verhalende video's die mensen zin geven om parken in het echt te bezoeken.",
      p2: 'Mijn sterkste community zit op TikTok, met content die eerst rond Europa-Park draaide en nu is uitgebreid naar grote Europese bestemmingen. Ik heb al content gemaakt in Energylandia, Efteling, Toverland en Universal Orlando.',
      p3: 'Mijn doel is eenvoudig: moderne, authentieke en performante content leveren die jullie parkbeleving zichtbaar maakt voor een gepassioneerd Frans publiek.',
      photoAlt: 'Baptiste in een pretpark'
    },
    partners: { title: 'Officiële parkpartnerschappen', subtitle: 'Betrouwbare samenwerkingen met toonaangevende Europese parken.' },
    nonPartners: { title: 'Parken waar ik filmde (zonder samenwerking)', subtitle: "Getoond in mijn video's zonder officiële samenwerking." },
    cases: { viewsLabel: 'views totaal', previous: 'Vorige', next: 'Volgende' },
    myVideos: {
      title: "Mijn video's",
      p1: "Ik publiceer gemiddeld één video per dag, gericht op een park of attractie, op basis van actuele socialmediatrends. Mijn formats omvatten ride-POV's, reactiecontent en meeslepende sequenties die in de eerste seconden de aandacht grijpen.",
      p2: 'Mijn redactionele stijl steunt op hooks die interactie stimuleren: parkherinneringen, vrienden taggen voor een volgend bezoek, observaties over bezoekersgedrag, praktische tips en vergelijkingen tussen attracties in verschillende parken.',
      gearTitle: 'Mijn filmuitrusting',
      gearNote: 'Ik maak al mijn content met deze uitrusting, die mijn belangrijkste dagelijkse filmsetup vormt.'
    },
    gear: { harness: 'Borstharnas', pole: 'Selfiestick' },
    contact: {
      title: 'Contact',
      subtitle: 'Voor samenwerkingen kun je contact met mij opnemen via deze verschillende adressen.',
      phoneLabel: 'Telefoon',
      reachOut: 'Je kunt contact met me opnemen via TikTok, e-mail, telefoonnummer en Instagram.'
    },
    videos: {
      back: 'Terug naar home', parkTitle: 'Park', parkLead: "Alle beschikbare video's en prestaties voor dit park.",
      viewBoth: 'TikTok + YouTube', viewTikTok: 'TikTok', viewYouTube: 'YouTube', platformFilter: 'Platform',
      layoutLabel: 'Weergave', layoutStack: 'Lijst', layoutDuo: 'Naast elkaar', layoutCompact: 'Compact',
      totalsTitle: 'Totale statistieken', totalVideos: "video's", totalViews: 'weergaven', totalLikes: 'likes', totalComments: 'reacties',
      noVideos: "Nog geen video's beschikbaar.", noHashtag: 'Geen hashtag gedetecteerd', linkUnavailable: 'Link niet beschikbaar',
      loading: 'Statistieken laden...', loadError: 'De statistieken konden niet worden geladen. Herlaad de pagina om het opnieuw te proberen.',
      noParkSelected: 'Geen park geselecteerd.', comingSoon: 'Videodetails voor dit park komen binnenkort.',
      mapCta: 'Openen in Google Maps', mapTitle: 'Parkkaart', thumbAlt: 'Videominiatuur',
      viewsUnit: 'weergaven', likesUnit: 'likes', commentsUnit: 'reacties'
    }
  },
  pl: {
    "titles": {
      "home": "Clip2EP",
      "videos": "Filmy z parku | Clip2EP"
    },
    "meta": {
      "home": "Oficjalna strona Clip2EP – twórca treści o parkach rozrywki na TikToku i YouTube."
    },
    "nav": {
      "label": "Nawigacja główna",
      "open": "Otwórz menu",
      "language": "Wybierz język",
      "home": "Start",
      "stats": "Statystyki",
      "about": "O mnie",
      "partners": "Partnerstwa",
      "videos": "Moje filmy",
      "contact": "Kontakt"
    },
    "common": {
      "footerMain": "© Clip2EP - 2026",
      "skip": "Przejdź do treści"
    },
    "park": {
      "station": "Stacja",
      "welcome": "Witamy w parku",
      "welcomeAfter": "",
      "editorial": "Moja linia redakcyjna",
      "badgeRole": "Twórca treści",
      "stampPartner": "Oficjalny partner",
      "stampVisited": "Odwiedzony",
      "ticket": "Bilet nr",
      "gearSign": "Sprzęt dozwolony na pokładzie",
      "booth": "Punkt informacyjny",
      "exit": "Wyjście",
      "bye": "Do zobaczenia!",
      "map": "Plan parku",
      "here": "Jesteś tutaj",
      "zone": {
        "entrance": "Wejście",
        "shop": "Sklep",
        "stats": "Wieża rekordów",
        "about": "Diabelski młyn",
        "partners": "Zamek partnerów",
        "videos": "Rollercoaster",
        "contact": "Kiosk informacyjny"
      },
      "mapHint": "Kliknij atrakcję, aby ją odwiedzić. Przeciągnij, aby obrócić i przechylić park.",
      "mapAria": "Interaktywny plan 3D parku Clip2EP",
      "rotateLeft": "Obróć park w lewo",
      "rotateRight": "Obróć park w prawo",
      "screen": "Na żywo z atrakcji",
      "footerThanks": "Dziękujemy za wizytę w parku Clip2EP! W sprawie współpracy punkt informacyjny jest otwarty przez cały rok.",
      "stationsTitle": "Stacje",
      "follow": "Obserwuj Clip2EP",
      "backTop": "Powrót do wejścia",
      "loading": "Otwieranie parku…"
    },
    "home": {
      "videoAria": "Film w tle z parku rozrywki",
      "title": "Jeden z największych francuskich twórców treści o parkach rozrywki",
      "lead": "+150 000 obserwujących na TikToku i +50 000 na YouTube, angażujące treści o największych parkach rozrywki na świecie!"
    },
    "stats": {
      "heading": "Statystyki odbiorców",
      "subheading": "Aktualny przegląd najnowszych danych.",
      "gender": "Podział według płci",
      "age": "Grupy wiekowe",
      "regions": "Najważniejsze regiony",
      "age65": "65 lat i więcej",
      "genderLabels": {
        "men": "Mężczyźni",
        "women": "Kobiety",
        "other": "Inne",
        "userSpecified": "Określone przez użytkownika"
      },
      "region": {
        "france": "Francja",
        "germany": "Niemcy",
        "belgium": "Belgia",
        "switzerland": "Szwajcaria",
        "usa": "Stany Zjednoczone",
        "canada": "Kanada",
        "reunion": "Reunion"
      },
      "tiktok": {
        "since": "Od 2023",
        "kpiViews": "Wyświetleń rocznie",
        "kpiFollowers": "Obserwujących"
      },
      "youtube": {
        "since": "Od 25 wrz 2025",
        "kpiViews": "Łączne wyświetlenia",
        "kpiFollowers": "Subskrybentów"
      }
    },
    "aboutme": {
      "kicker": "O mnie",
      "title": "Twórca treści o parkach rozrywki",
      "p1": "Nazywam się <strong>Baptiste DIDIER</strong> i mam 19 lat. Studiuję informatykę w Belfort, a wolny czas poświęcam na tworzenie treści o <strong>parkach rozrywki</strong>. Codziennie publikuję krótkie, dynamiczne i angażujące filmy, skupione na praktycznych poradach i wspomnieniach z wizyt.",
      "p2": "Moja najsilniejsza społeczność jest na <strong>TikToku</strong> – na początku skupiałem się na <strong>Europa-Park</strong>, a potem rozszerzyłem treści na duże europejskie kierunki. Tworzyłem już materiały w wielu dużych parkach, z reagującą i wierną publicznością.",
      "p3": "Mój cel jest prosty: tworzyć <strong>nowoczesne, autentyczne i skuteczne</strong> treści, zgodne z aktualnymi trendami, aby promować Twój park wśród pasjonatów z Francji.",
      "photoAlt": "Baptiste w parku rozrywki"
    },
    "partners": {
      "title": "Parki z oficjalnym partnerstwem",
      "subtitle": "Potwierdzone współprace z uznanymi europejskimi parkami."
    },
    "nonPartners": {
      "title": "Parki odwiedzone bez partnerstwa",
      "subtitle": "Pokazane w moich filmach bez oficjalnego partnerstwa."
    },
    "cases": {
      "viewsLabel": "wyświetleń łącznie",
      "previous": "Poprzedni",
      "next": "Następny"
    },
    "myVideos": {
      "title": "Moje filmy",
      "p1": "Publikuję średnio <strong>jeden film dziennie</strong> o parku lub atrakcji, opierając się na aktualnych trendach w mediach społecznościowych. Moje formaty to m.in. <strong>POV z atrakcji</strong>, reakcje i immersyjne sekwencje, które przyciągają uwagę od pierwszych sekund.",
      "p2": "Moja linia redakcyjna opiera się na haczykach, które zachęcają do interakcji: wspomnienia z parków, oznaczanie znajomych przed kolejną wizytą, obserwacje zachowań odwiedzających, praktyczne wskazówki i porównania atrakcji w różnych parkach.",
      "gearTitle": "Mój sprzęt do nagrywania",
      "gearNote": "Wszystkie moje treści tworzę tym sprzętem – to mój główny codzienny zestaw do nagrywania."
    },
    "gear": {
      "harness": "Szelki na klatkę",
      "pole": "Kij"
    },
    "contact": {
      "title": "Kontakt",
      "subtitle": "W sprawie partnerstw możesz skontaktować się ze mną przez poniższe kanały",
      "phoneLabel": "Telefon",
      "reachOut": "Możesz skontaktować się ze mną przez TikToka, e-mail, telefon i Instagram."
    },
    "videos": {
      "back": "Powrót do strony głównej",
      "parkTitle": "Park",
      "parkLead": "Wszystkie dostępne filmy i wyniki dla tego parku.",
      "viewBoth": "TikTok + YouTube",
      "viewTikTok": "TikTok",
      "viewYouTube": "YouTube",
      "platformFilter": "Platforma",
      "layoutLabel": "Widok",
      "layoutStack": "Lista",
      "layoutDuo": "Obok siebie",
      "layoutCompact": "Kompaktowy",
      "totalsTitle": "Statystyki parku",
      "totalVideos": "filmy",
      "totalViews": "wyświetlenia",
      "totalLikes": "polubienia",
      "totalComments": "komentarze",
      "noVideos": "Brak filmów na razie.",
      "noHashtag": "Nie wykryto hashtagów",
      "linkUnavailable": "Link niedostępny",
      "loading": "Ładowanie statystyk...",
      "loadError": "Nie udało się wczytać statystyk. Odśwież stronę, aby spróbować ponownie.",
      "noParkSelected": "Nie wybrano parku.",
      "comingSoon": "Szczegóły filmów dla tego parku już wkrótce.",
      "mapCta": "Otwórz w Mapach Google",
      "mapTitle": "Mapa parku",
      "thumbAlt": "Miniatura filmu",
      "viewsUnit": "wyświetleń",
      "likesUnit": "polubień",
      "commentsUnit": "komentarzy"
    }
  },
  it: {
    "titles": {
      "home": "Clip2EP",
      "videos": "Video del parco | Clip2EP"
    },
    "meta": {
      "home": "Sito ufficiale di Clip2EP – creatore di contenuti sui parchi divertimento su TikTok e YouTube."
    },
    "nav": {
      "label": "Navigazione principale",
      "open": "Apri il menu",
      "language": "Scegli la lingua",
      "home": "Home",
      "stats": "Statistiche",
      "about": "Chi sono",
      "partners": "Partnership",
      "videos": "I miei video",
      "contact": "Contatti"
    },
    "common": {
      "footerMain": "© Clip2EP - 2026",
      "skip": "Vai al contenuto"
    },
    "park": {
      "station": "Stazione",
      "welcome": "Benvenuti nel parco",
      "welcomeAfter": "",
      "editorial": "La mia linea editoriale",
      "badgeRole": "Creatore di contenuti",
      "stampPartner": "Partner ufficiale",
      "stampVisited": "Visitato",
      "ticket": "Biglietto n.",
      "gearSign": "Attrezzatura ammessa a bordo",
      "booth": "Punto informazioni",
      "exit": "Uscita",
      "bye": "A presto!",
      "map": "Mappa del parco",
      "here": "Tu sei qui",
      "zone": {
        "entrance": "L'Ingresso",
        "shop": "Il Negozio",
        "stats": "La Torre dei record",
        "about": "La Ruota panoramica",
        "partners": "Il Castello dei partner",
        "videos": "Le Montagne russe",
        "contact": "Il Chiosco info"
      },
      "mapHint": "Clicca su un'attrazione per visitarla. Trascina per ruotare e inclinare il parco.",
      "mapAria": "Mappa 3D interattiva del parco Clip2EP",
      "rotateLeft": "Ruota il parco a sinistra",
      "rotateRight": "Ruota il parco a destra",
      "screen": "In diretta dall'attrazione",
      "footerThanks": "Grazie per aver visitato il parco Clip2EP! Per una collaborazione, il punto informazioni è aperto tutto l'anno.",
      "stationsTitle": "Le stazioni",
      "follow": "Segui Clip2EP",
      "backTop": "Torna all'ingresso",
      "loading": "Apertura del parco…"
    },
    "home": {
      "videoAria": "Video di sfondo di un parco divertimenti",
      "title": "Uno dei più grandi creatori francesi di contenuti dedicati ai parchi divertimento",
      "lead": "+150.000 follower su TikTok e +50.000 su YouTube, contenuti ad alto coinvolgimento sui più grandi parchi divertimento del mondo!"
    },
    "stats": {
      "heading": "Statistiche del pubblico",
      "subheading": "Panoramica aggiornata degli ultimi dati.",
      "gender": "Ripartizione per genere",
      "age": "Fasce d'età",
      "regions": "Regioni principali",
      "age65": "65 anni e oltre",
      "genderLabels": {
        "men": "Uomini",
        "women": "Donne",
        "other": "Altro",
        "userSpecified": "Specificato dall'utente"
      },
      "region": {
        "france": "Francia",
        "germany": "Germania",
        "belgium": "Belgio",
        "switzerland": "Svizzera",
        "usa": "Stati Uniti",
        "canada": "Canada",
        "reunion": "Riunione"
      },
      "tiktok": {
        "since": "Dal 2023",
        "kpiViews": "Visualizzazioni all'anno",
        "kpiFollowers": "Follower"
      },
      "youtube": {
        "since": "Dal 25 set 2025",
        "kpiViews": "Visualizzazioni totali",
        "kpiFollowers": "Iscritti"
      }
    },
    "aboutme": {
      "kicker": "Chi sono",
      "title": "Creatore di contenuti sui parchi divertimento",
      "p1": "Mi chiamo <strong>Baptiste DIDIER</strong> e ho 19 anni. Studio informatica a Belfort e dedico il mio tempo libero alla creazione di contenuti sui <strong>parchi divertimento</strong>. Pubblico ogni giorno video brevi, dinamici e coinvolgenti, con un taglio editoriale fatto di consigli pratici e ricordi di visita.",
      "p2": "La mia community più forte è su <strong>TikTok</strong>, con contenuti inizialmente dedicati a <strong>Europa-Park</strong> e poi estesi alle grandi destinazioni europee. Ho già realizzato contenuti in diversi parchi importanti, con un pubblico reattivo e fedele.",
      "p3": "Il mio obiettivo è semplice: produrre contenuti <strong>moderni, autentici ed efficaci</strong>, seguendo le tendenze del momento, per valorizzare il tuo parco presso un pubblico francese appassionato.",
      "photoAlt": "Baptiste in un parco divertimenti"
    },
    "partners": {
      "title": "Parchi in partnership ufficiale",
      "subtitle": "Collaborazioni confermate con parchi europei rinomati."
    },
    "nonPartners": {
      "title": "Parchi visitati senza partnership",
      "subtitle": "Presenti nei miei video senza partnership ufficiale."
    },
    "cases": {
      "viewsLabel": "visualizzazioni totali",
      "previous": "Precedente",
      "next": "Successivo"
    },
    "myVideos": {
      "title": "I miei video",
      "p1": "Pubblico in media <strong>un video al giorno</strong>, dedicato a un parco o a un'attrazione, seguendo le tendenze del momento sui social. I miei formati includono <strong>POV delle attrazioni</strong>, contenuti reaction e sequenze immersive pensate per catturare l'attenzione fin dai primi secondi.",
      "p2": "La mia linea editoriale si basa su spunti che favoriscono l'interazione: ricordi legati ai parchi, tag degli amici per la prossima visita, osservazioni sul comportamento dei visitatori, consigli pratici e confronti tra attrazioni di parchi diversi.",
      "gearTitle": "La mia attrezzatura video",
      "gearNote": "Realizzo tutti i miei contenuti con questa attrezzatura, che è il mio set principale di ripresa quotidiano."
    },
    "gear": {
      "harness": "Imbracatura da petto",
      "pole": "Asta"
    },
    "contact": {
      "title": "Contatti",
      "subtitle": "Per le partnership puoi contattarmi tramite questi recapiti",
      "phoneLabel": "Telefono",
      "reachOut": "Puoi contattarmi tramite TikTok, e-mail, telefono e Instagram."
    },
    "videos": {
      "back": "Torna alla home",
      "parkTitle": "Parco",
      "parkLead": "Tutti i video e i risultati disponibili per questo parco.",
      "viewBoth": "TikTok + YouTube",
      "viewTikTok": "TikTok",
      "viewYouTube": "YouTube",
      "platformFilter": "Piattaforma",
      "layoutLabel": "Visualizzazione",
      "layoutStack": "Elenco",
      "layoutDuo": "Affiancati",
      "layoutCompact": "Compatto",
      "totalsTitle": "Statistiche del parco",
      "totalVideos": "video",
      "totalViews": "visualizzazioni",
      "totalLikes": "mi piace",
      "totalComments": "commenti",
      "noVideos": "Nessun video disponibile al momento.",
      "noHashtag": "Nessun hashtag rilevato",
      "linkUnavailable": "Link non disponibile",
      "loading": "Caricamento delle statistiche...",
      "loadError": "Impossibile caricare le statistiche. Ricarica la pagina per riprovare.",
      "noParkSelected": "Nessun parco selezionato.",
      "comingSoon": "I dettagli dei video di questo parco arriveranno presto.",
      "mapCta": "Apri in Google Maps",
      "mapTitle": "Mappa del parco",
      "thumbAlt": "Miniatura del video",
      "viewsUnit": "visualizzazioni",
      "likesUnit": "mi piace",
      "commentsUnit": "commenti"
    }
  },
  sv: {
    "titles": {
      "home": "Clip2EP",
      "videos": "Parkvideor | Clip2EP"
    },
    "meta": {
      "home": "Officiell webbplats för Clip2EP – innehållsskapare om nöjesparker på TikTok och YouTube."
    },
    "nav": {
      "label": "Huvudnavigering",
      "open": "Öppna menyn",
      "language": "Välj språk",
      "home": "Start",
      "stats": "Statistik",
      "about": "Om mig",
      "partners": "Samarbeten",
      "videos": "Mina videor",
      "contact": "Kontakt"
    },
    "common": {
      "footerMain": "© Clip2EP - 2026",
      "skip": "Hoppa till innehållet"
    },
    "park": {
      "station": "Station",
      "welcome": "Välkommen till",
      "welcomeAfter": "-parken",
      "editorial": "Min redaktionella linje",
      "badgeRole": "Innehållsskapare",
      "stampPartner": "Officiell partner",
      "stampVisited": "Besökt",
      "ticket": "Biljett nr",
      "gearSign": "Tillåten utrustning ombord",
      "booth": "Informationspunkt",
      "exit": "Utgång",
      "bye": "Vi ses snart!",
      "map": "Parkkarta",
      "here": "Du är här",
      "zone": {
        "entrance": "Entrén",
        "shop": "Butiken",
        "stats": "Rekordtornet",
        "about": "Pariserhjulet",
        "partners": "Partnerslottet",
        "videos": "Berg-och-dalbanan",
        "contact": "Infokiosken"
      },
      "mapHint": "Klicka på en attraktion för att besöka den. Dra för att vrida och luta parken.",
      "mapAria": "Interaktiv 3D-karta över Clip2EP-parken",
      "rotateLeft": "Vrid parken åt vänster",
      "rotateRight": "Vrid parken åt höger",
      "screen": "Direkt från attraktionen",
      "footerThanks": "Tack för ditt besök i Clip2EP-parken! För samarbeten är informationspunkten öppen året runt.",
      "stationsTitle": "Stationerna",
      "follow": "Följ Clip2EP",
      "backTop": "Tillbaka till entrén",
      "loading": "Parken öppnar…"
    },
    "home": {
      "videoAria": "Bakgrundsvideo från en nöjespark",
      "title": "En av de största franska innehållsskaparna om nöjesparker",
      "lead": "+150 000 följare på TikTok och +50 000 på YouTube, engagerande innehåll om världens största nöjesparker!"
    },
    "stats": {
      "heading": "Publikstatistik",
      "subheading": "Uppdaterad översikt av de senaste insikterna.",
      "gender": "Fördelning per kön",
      "age": "Åldersgrupper",
      "regions": "Toppregioner",
      "age65": "65 år och äldre",
      "genderLabels": {
        "men": "Män",
        "women": "Kvinnor",
        "other": "Annat",
        "userSpecified": "Angivet av användaren"
      },
      "region": {
        "france": "Frankrike",
        "germany": "Tyskland",
        "belgium": "Belgien",
        "switzerland": "Schweiz",
        "usa": "USA",
        "canada": "Kanada",
        "reunion": "Réunion"
      },
      "tiktok": {
        "since": "Sedan 2023",
        "kpiViews": "Visningar per år",
        "kpiFollowers": "Följare"
      },
      "youtube": {
        "since": "Sedan 25 sep 2025",
        "kpiViews": "Totala visningar",
        "kpiFollowers": "Prenumeranter"
      }
    },
    "aboutme": {
      "kicker": "Om mig",
      "title": "Innehållsskapare om nöjesparker",
      "p1": "Jag heter <strong>Baptiste DIDIER</strong> och är 19 år. Jag studerar datavetenskap i Belfort och ägnar min fritid åt att skapa innehåll om <strong>nöjesparker</strong>. Jag publicerar korta, dynamiska och engagerande videor varje dag, med fokus på praktiska tips och minnen från besök.",
      "p2": "Min starkaste community finns på <strong>TikTok</strong>, med innehåll som först handlade om <strong>Europa-Park</strong> och sedan breddades till stora europeiska resmål. Jag har redan skapat innehåll i flera stora parker, med en engagerad och trogen publik.",
      "p3": "Mitt mål är enkelt: skapa <strong>moderna, äkta och effektiva</strong> videor som följer dagens trender, för att lyfta fram din park för en passionerad fransk publik.",
      "photoAlt": "Baptiste i en nöjespark"
    },
    "partners": {
      "title": "Parker med officiellt samarbete",
      "subtitle": "Bekräftade samarbeten med välkända europeiska parker."
    },
    "nonPartners": {
      "title": "Parker besökta utan samarbete",
      "subtitle": "Med i mina videor utan officiellt samarbete."
    },
    "cases": {
      "viewsLabel": "visningar totalt",
      "previous": "Föregående",
      "next": "Nästa"
    },
    "myVideos": {
      "title": "Mina videor",
      "p1": "Jag publicerar i genomsnitt <strong>en video om dagen</strong> om en park eller attraktion, baserat på aktuella trender i sociala medier. Mina format omfattar bland annat <strong>POV från attraktioner</strong>, reaktionsinnehåll och uppslukande sekvenser som fångar uppmärksamheten från första sekunden.",
      "p2": "Min redaktionella linje bygger på krokar som skapar interaktion: minnen från parker, att tagga vänner inför nästa besök, iakttagelser om besökares beteende, praktiska tips och jämförelser mellan attraktioner i olika parker.",
      "gearTitle": "Min filmutrustning",
      "gearNote": "Jag skapar allt mitt innehåll med den här utrustningen, som är min huvudsakliga vardagliga filmutrustning."
    },
    "gear": {
      "harness": "Bröstsele",
      "pole": "Stång"
    },
    "contact": {
      "title": "Kontakt",
      "subtitle": "För samarbeten kan du kontakta mig via dessa kanaler",
      "phoneLabel": "Telefon",
      "reachOut": "Du kan kontakta mig via TikTok, e-post, telefon och Instagram."
    },
    "videos": {
      "back": "Tillbaka till startsidan",
      "parkTitle": "Park",
      "parkLead": "Alla tillgängliga videor och resultat för den här parken.",
      "viewBoth": "TikTok + YouTube",
      "viewTikTok": "TikTok",
      "viewYouTube": "YouTube",
      "platformFilter": "Plattform",
      "layoutLabel": "Visning",
      "layoutStack": "Lista",
      "layoutDuo": "Sida vid sida",
      "layoutCompact": "Kompakt",
      "totalsTitle": "Parkstatistik",
      "totalVideos": "videor",
      "totalViews": "visningar",
      "totalLikes": "gilla-markeringar",
      "totalComments": "kommentarer",
      "noVideos": "Inga videor ännu.",
      "noHashtag": "Inga hashtaggar hittades",
      "linkUnavailable": "Länken är inte tillgänglig",
      "loading": "Laddar statistik...",
      "loadError": "Statistiken kunde inte laddas. Ladda om sidan för att försöka igen.",
      "noParkSelected": "Ingen park vald.",
      "comingSoon": "Videodetaljer för den här parken kommer snart.",
      "mapCta": "Öppna i Google Maps",
      "mapTitle": "Karta över parken",
      "thumbAlt": "Videominiatyr",
      "viewsUnit": "visningar",
      "likesUnit": "gilla",
      "commentsUnit": "kommentarer"
    }
  },
  ar: {
    "titles": {
      "home": "Clip2EP",
      "videos": "فيديوهات المنتزه | Clip2EP"
    },
    "meta": {
      "home": "الموقع الرسمي لـ Clip2EP – صانع محتوى عن مدن الملاهي على تيك توك ويوتيوب."
    },
    "nav": {
      "label": "التنقل الرئيسي",
      "open": "فتح القائمة",
      "language": "اختر اللغة",
      "home": "الرئيسية",
      "stats": "الإحصائيات",
      "about": "من أنا",
      "partners": "الشراكات",
      "videos": "فيديوهاتي",
      "contact": "تواصل"
    },
    "common": {
      "footerMain": "© Clip2EP - 2026",
      "skip": "الانتقال إلى المحتوى"
    },
    "park": {
      "station": "محطة",
      "welcome": "مرحبًا بك في منتزه",
      "welcomeAfter": "",
      "editorial": "خطي التحريري",
      "badgeRole": "صانع محتوى",
      "stampPartner": "شريك رسمي",
      "stampVisited": "تمت زيارته",
      "ticket": "تذكرة رقم",
      "gearSign": "المعدات المسموح بها على متن اللعبة",
      "booth": "نقطة معلومات",
      "exit": "خروج",
      "bye": "إلى اللقاء!",
      "map": "خريطة المنتزه",
      "here": "أنت هنا",
      "zone": {
        "entrance": "المدخل",
        "shop": "المتجر",
        "stats": "برج الأرقام القياسية",
        "about": "العجلة الدوارة",
        "partners": "قلعة الشركاء",
        "videos": "الأفعوانية",
        "contact": "كشك المعلومات"
      },
      "mapHint": "انقر على لعبة لزيارتها. اسحب لتدوير المنتزه وإمالته.",
      "mapAria": "خريطة ثلاثية الأبعاد تفاعلية لمنتزه Clip2EP",
      "rotateLeft": "تدوير المنتزه إلى اليسار",
      "rotateRight": "تدوير المنتزه إلى اليمين",
      "screen": "بث مباشر من اللعبة",
      "footerThanks": "شكرًا لزيارتك منتزه Clip2EP! للتعاون، نقطة المعلومات مفتوحة طوال العام.",
      "stationsTitle": "المحطات",
      "follow": "تابع Clip2EP",
      "backTop": "العودة إلى المدخل",
      "loading": "جارٍ افتتاح المنتزه…"
    },
    "home": {
      "videoAria": "فيديو خلفية لمدينة ملاهٍ",
      "title": "واحد من أكبر صنّاع المحتوى الفرنسيين المتخصصين في مدن الملاهي",
      "lead": "أكثر من 150 000 متابع على تيك توك وأكثر من 50 000 على يوتيوب، ومحتوى عالي التفاعل عن أكبر مدن الملاهي في العالم!"
    },
    "stats": {
      "heading": "إحصائيات الجمهور",
      "subheading": "نظرة محدثة على أحدث البيانات.",
      "gender": "التوزيع حسب الجنس",
      "age": "الفئات العمرية",
      "regions": "أهم المناطق",
      "age65": "65 عامًا فأكثر",
      "genderLabels": {
        "men": "رجال",
        "women": "نساء",
        "other": "آخر",
        "userSpecified": "حدده المستخدم"
      },
      "region": {
        "france": "فرنسا",
        "germany": "ألمانيا",
        "belgium": "بلجيكا",
        "switzerland": "سويسرا",
        "usa": "الولايات المتحدة",
        "canada": "كندا",
        "reunion": "لا ريونيون"
      },
      "tiktok": {
        "since": "منذ 2023",
        "kpiViews": "مشاهدة سنويًا",
        "kpiFollowers": "متابع"
      },
      "youtube": {
        "since": "منذ 25 سبتمبر 2025",
        "kpiViews": "إجمالي المشاهدات",
        "kpiFollowers": "مشترك"
      }
    },
    "aboutme": {
      "kicker": "من أنا",
      "title": "صانع محتوى عن مدن الملاهي",
      "p1": "اسمي <strong>باتيست ديدييه</strong> وعمري 19 عامًا. أدرس علوم الحاسوب في بلفور، وأخصص وقت فراغي لصناعة محتوى عن <strong>مدن الملاهي</strong>. أنشر كل يوم فيديوهات قصيرة وحيوية وجذابة، بأسلوب يركز على النصائح العملية وذكريات الزيارات.",
      "p2": "أقوى مجتمع لدي على <strong>تيك توك</strong>، حيث بدأ المحتوى بالتركيز على <strong>يوروبا بارك</strong> ثم توسع إلى وجهات أوروبية كبرى. صنعت بالفعل محتوى في عدة منتزهات كبيرة مع جمهور متفاعل ووفي.",
      "p3": "هدفي بسيط: إنتاج محتوى <strong>عصري وأصيل وفعّال</strong> يواكب الاتجاهات الحالية، لإبراز منتزهك أمام جمهور فرنسي شغوف.",
      "photoAlt": "باتيست في مدينة ملاهٍ"
    },
    "partners": {
      "title": "منتزهات بشراكة رسمية",
      "subtitle": "تعاونات مؤكدة مع منتزهات أوروبية معروفة."
    },
    "nonPartners": {
      "title": "منتزهات زرتها دون شراكة",
      "subtitle": "ظهرت في فيديوهاتي دون شراكة رسمية."
    },
    "cases": {
      "viewsLabel": "مشاهدة إجمالًا",
      "previous": "السابق",
      "next": "التالي"
    },
    "myVideos": {
      "title": "فيديوهاتي",
      "p1": "أنشر في المتوسط <strong>فيديو واحدًا يوميًا</strong> عن منتزه أو لعبة، مستندًا إلى اتجاهات مواقع التواصل الحالية. تشمل صيغي <strong>لقطات من منظور الراكب</strong> ومحتوى ردود الفعل ومشاهد غامرة مصممة لجذب الانتباه من الثواني الأولى.",
      "p2": "يعتمد خطي التحريري على مداخل تشجع التفاعل: ذكريات المنتزهات، والإشارة إلى الأصدقاء للزيارة القادمة، وملاحظات عن سلوك الزوار، ونصائح عملية، ومقارنات بين ألعاب منتزهات مختلفة.",
      "gearTitle": "معدات التصوير",
      "gearNote": "أصنع كل محتواي بهذه المعدات، وهي عدتي الأساسية للتصوير اليومي."
    },
    "gear": {
      "harness": "حزام الصدر",
      "pole": "عصا تصوير"
    },
    "contact": {
      "title": "تواصل",
      "subtitle": "للشراكات يمكنك التواصل معي عبر هذه القنوات",
      "phoneLabel": "الهاتف",
      "reachOut": "يمكنك التواصل معي عبر تيك توك والبريد الإلكتروني والهاتف وإنستغرام."
    },
    "videos": {
      "back": "العودة إلى الرئيسية",
      "parkTitle": "منتزه",
      "parkLead": "كل الفيديوهات والنتائج المتاحة لهذا المنتزه.",
      "viewBoth": "TikTok + YouTube",
      "viewTikTok": "TikTok",
      "viewYouTube": "YouTube",
      "platformFilter": "المنصة",
      "layoutLabel": "العرض",
      "layoutStack": "قائمة",
      "layoutDuo": "جنبًا إلى جنب",
      "layoutCompact": "مضغوط",
      "totalsTitle": "إحصائيات المنتزه",
      "totalVideos": "فيديو",
      "totalViews": "مشاهدة",
      "totalLikes": "إعجاب",
      "totalComments": "تعليق",
      "noVideos": "لا توجد فيديوهات حاليًا.",
      "noHashtag": "لم يتم العثور على وسوم",
      "linkUnavailable": "الرابط غير متاح",
      "loading": "جارٍ تحميل الإحصائيات...",
      "loadError": "تعذر تحميل الإحصائيات. أعد تحميل الصفحة للمحاولة مجددًا.",
      "noParkSelected": "لم يتم اختيار منتزه.",
      "comingSoon": "تفاصيل فيديوهات هذا المنتزه قريبًا.",
      "mapCta": "فتح في خرائط Google",
      "mapTitle": "خريطة المنتزه",
      "thumbAlt": "صورة مصغرة للفيديو",
      "viewsUnit": "مشاهدة",
      "likesUnit": "إعجاب",
      "commentsUnit": "تعليق"
    }
  },
  zh: {
    "titles": {
      "home": "Clip2EP",
      "videos": "乐园视频 | Clip2EP"
    },
    "meta": {
      "home": "Clip2EP 官方网站——TikTok 与 YouTube 上的主题乐园内容创作者。"
    },
    "nav": {
      "label": "主导航",
      "open": "打开菜单",
      "language": "选择语言",
      "home": "首页",
      "stats": "数据",
      "about": "关于我",
      "partners": "合作",
      "videos": "我的视频",
      "contact": "联系"
    },
    "common": {
      "footerMain": "© Clip2EP - 2026",
      "skip": "跳到内容"
    },
    "park": {
      "station": "站点",
      "welcome": "欢迎来到",
      "welcomeAfter": " 乐园",
      "editorial": "我的内容方向",
      "badgeRole": "内容创作者",
      "stampPartner": "官方合作伙伴",
      "stampVisited": "已到访",
      "ticket": "票号",
      "gearSign": "允许随身携带的设备",
      "booth": "服务台",
      "exit": "出口",
      "bye": "下次见！",
      "map": "乐园地图",
      "here": "你在这里",
      "zone": {
        "entrance": "入口",
        "shop": "商店",
        "stats": "纪录塔",
        "about": "摩天轮",
        "partners": "合作伙伴城堡",
        "videos": "过山车",
        "contact": "信息亭"
      },
      "mapHint": "点击游乐设施即可参观。拖动可旋转和倾斜乐园。",
      "mapAria": "Clip2EP 乐园交互式 3D 地图",
      "rotateLeft": "向左旋转乐园",
      "rotateRight": "向右旋转乐园",
      "screen": "游乐设施实况",
      "footerThanks": "感谢参观 Clip2EP 乐园！如需合作，服务台全年开放。",
      "stationsTitle": "站点",
      "follow": "关注 Clip2EP",
      "backTop": "返回入口",
      "loading": "乐园开园中…"
    },
    "home": {
      "videoAria": "主题乐园背景视频",
      "title": "法国最大的主题乐园内容创作者之一",
      "lead": "TikTok 粉丝超过 150 000，YouTube 粉丝超过 50 000，围绕全球顶级主题乐园打造高互动内容！"
    },
    "stats": {
      "heading": "受众数据",
      "subheading": "最新数据概览。",
      "gender": "性别分布",
      "age": "年龄段",
      "regions": "主要地区",
      "age65": "65 岁及以上",
      "genderLabels": {
        "men": "男性",
        "women": "女性",
        "other": "其他",
        "userSpecified": "用户自定义"
      },
      "region": {
        "france": "法国",
        "germany": "德国",
        "belgium": "比利时",
        "switzerland": "瑞士",
        "usa": "美国",
        "canada": "加拿大",
        "reunion": "留尼汪"
      },
      "tiktok": {
        "since": "自 2023 年",
        "kpiViews": "年播放量",
        "kpiFollowers": "粉丝"
      },
      "youtube": {
        "since": "自 2025 年 9 月 25 日",
        "kpiViews": "总播放量",
        "kpiFollowers": "订阅者"
      }
    },
    "aboutme": {
      "kicker": "关于我",
      "title": "主题乐园内容创作者",
      "p1": "我叫 <strong>Baptiste DIDIER</strong>，今年 19 岁。我目前在贝尔福学习计算机，业余时间专注于创作<strong>主题乐园</strong>内容。我每天发布节奏明快、富有吸引力的短视频，内容侧重实用建议和游玩回忆。",
      "p2": "我最强大的社区在 <strong>TikTok</strong>，内容最初聚焦于 <strong>Europa-Park</strong>，随后扩展到欧洲各大目的地。我已经在多个大型乐园拍摄过内容，观众反响热烈且忠实。",
      "p3": "我的目标很简单：紧跟当下潮流，制作<strong>现代、真实、高效</strong>的内容，向热情的法国观众展示你的乐园。",
      "photoAlt": "Baptiste 在主题乐园"
    },
    "partners": {
      "title": "官方合作乐园",
      "subtitle": "与知名欧洲乐园的已确认合作。"
    },
    "nonPartners": {
      "title": "未合作但到访过的乐园",
      "subtitle": "在我的视频中出现，但没有官方合作。"
    },
    "cases": {
      "viewsLabel": "总播放量",
      "previous": "上一个",
      "next": "下一个"
    },
    "myVideos": {
      "title": "我的视频",
      "p1": "我平均<strong>每天发布一条视频</strong>，主题是某个乐园或游乐设施，紧跟社交媒体的当下潮流。我的形式包括<strong>游乐设施第一视角</strong>、反应类内容，以及从第一秒就抓住注意力的沉浸式片段。",
      "p2": "我的内容方向以促进互动的切入点为主：乐园回忆、@好友约下次同游、游客行为观察、实用小贴士，以及不同乐园游乐设施的对比。",
      "gearTitle": "我的拍摄设备",
      "gearNote": "我所有的内容都用这套设备拍摄，这是我日常的主力拍摄装备。"
    },
    "gear": {
      "harness": "胸前支架",
      "pole": "自拍杆"
    },
    "contact": {
      "title": "联系",
      "subtitle": "如需合作，可以通过以下方式联系我",
      "phoneLabel": "电话",
      "reachOut": "你可以通过 TikTok、邮件、电话和 Instagram 联系我。"
    },
    "videos": {
      "back": "返回首页",
      "parkTitle": "乐园",
      "parkLead": "该乐园的全部视频与数据。",
      "viewBoth": "TikTok + YouTube",
      "viewTikTok": "TikTok",
      "viewYouTube": "YouTube",
      "platformFilter": "平台",
      "layoutLabel": "显示方式",
      "layoutStack": "列表",
      "layoutDuo": "并排",
      "layoutCompact": "紧凑",
      "totalsTitle": "乐园数据",
      "totalVideos": "条视频",
      "totalViews": "次播放",
      "totalLikes": "个赞",
      "totalComments": "条评论",
      "noVideos": "暂时没有视频。",
      "noHashtag": "未检测到话题标签",
      "linkUnavailable": "链接不可用",
      "loading": "正在加载数据...",
      "loadError": "无法加载数据。请刷新页面重试。",
      "noParkSelected": "未选择乐园。",
      "comingSoon": "该乐园的视频详情即将上线。",
      "mapCta": "在 Google 地图中打开",
      "mapTitle": "乐园地图",
      "thumbAlt": "视频缩略图",
      "viewsUnit": "次播放",
      "likesUnit": "个赞",
      "commentsUnit": "条评论"
    }
  },
  ja: {
    "titles": {
      "home": "Clip2EP",
      "videos": "パーク動画 | Clip2EP"
    },
    "meta": {
      "home": "Clip2EP 公式サイト – TikTok と YouTube で活動するテーマパーク専門のクリエイター。"
    },
    "nav": {
      "label": "メインナビゲーション",
      "open": "メニューを開く",
      "language": "言語を選択",
      "home": "ホーム",
      "stats": "統計",
      "about": "プロフィール",
      "partners": "パートナー",
      "videos": "動画",
      "contact": "お問い合わせ"
    },
    "common": {
      "footerMain": "© Clip2EP - 2026",
      "skip": "本文へ移動"
    },
    "park": {
      "station": "ステーション",
      "welcome": "ようこそ",
      "welcomeAfter": " パークへ",
      "editorial": "私の編集方針",
      "badgeRole": "コンテンツクリエイター",
      "stampPartner": "公式パートナー",
      "stampVisited": "訪問済み",
      "ticket": "チケット番号",
      "gearSign": "持ち込み可能な機材",
      "booth": "インフォメーション",
      "exit": "出口",
      "bye": "またね！",
      "map": "パークマップ",
      "here": "現在地",
      "zone": {
        "entrance": "エントランス",
        "shop": "ショップ",
        "stats": "記録の塔",
        "about": "観覧車",
        "partners": "パートナーの城",
        "videos": "ジェットコースター",
        "contact": "インフォメーション"
      },
      "mapHint": "アトラクションをクリックして訪問。ドラッグでパークを回転・傾けられます。",
      "mapAria": "Clip2EP パークのインタラクティブ 3D マップ",
      "rotateLeft": "パークを左に回転",
      "rotateRight": "パークを右に回転",
      "screen": "アトラクションから生中継",
      "footerThanks": "Clip2EP パークへのご来園ありがとうございます！コラボのご相談はインフォメーションで一年中受け付けています。",
      "stationsTitle": "ステーション",
      "follow": "Clip2EP をフォロー",
      "backTop": "エントランスに戻る",
      "loading": "パーク開園中…"
    },
    "home": {
      "videoAria": "テーマパークの背景動画",
      "title": "フランス最大級のテーマパーク専門コンテンツクリエイター",
      "lead": "TikTok で 150 000 人以上、YouTube で 50 000 人以上のフォロワー。世界の大型テーマパークをテーマにした高エンゲージメントのコンテンツ！"
    },
    "stats": {
      "heading": "オーディエンス統計",
      "subheading": "最新データの概要。",
      "gender": "性別の内訳",
      "age": "年齢層",
      "regions": "主な地域",
      "age65": "65 歳以上",
      "genderLabels": {
        "men": "男性",
        "women": "女性",
        "other": "その他",
        "userSpecified": "ユーザー指定"
      },
      "region": {
        "france": "フランス",
        "germany": "ドイツ",
        "belgium": "ベルギー",
        "switzerland": "スイス",
        "usa": "アメリカ",
        "canada": "カナダ",
        "reunion": "レユニオン"
      },
      "tiktok": {
        "since": "2023 年から",
        "kpiViews": "年間再生回数",
        "kpiFollowers": "フォロワー"
      },
      "youtube": {
        "since": "2025 年 9 月 25 日から",
        "kpiViews": "総再生回数",
        "kpiFollowers": "登録者"
      }
    },
    "aboutme": {
      "kicker": "プロフィール",
      "title": "テーマパーク専門のコンテンツクリエイター",
      "p1": "<strong>Baptiste DIDIER</strong> です。19 歳で、ベルフォールで情報工学を学びながら、空き時間に<strong>テーマパーク</strong>専門のコンテンツを制作しています。実用的なアドバイスや来園の思い出を軸に、テンポがよく引き込まれるショート動画を毎日投稿しています。",
      "p2": "最も大きなコミュニティは <strong>TikTok</strong> にあり、最初は <strong>Europa-Park</strong> を中心に、その後ヨーロッパの主要な行き先へと広げてきました。すでに複数の大型パークで撮影しており、反応がよく熱心な視聴者に支えられています。",
      "p3": "目標はシンプルです。今のトレンドに合わせた<strong>モダンで本物の、成果につながる</strong>コンテンツで、熱心なフランスの視聴者にあなたのパークの魅力を届けることです。",
      "photoAlt": "テーマパークにいる Baptiste"
    },
    "partners": {
      "title": "公式パートナーのパーク",
      "subtitle": "ヨーロッパの有名パークとの確定したコラボ。"
    },
    "nonPartners": {
      "title": "提携なしで訪れたパーク",
      "subtitle": "公式提携なしで私の動画に登場したパーク。"
    },
    "cases": {
      "viewsLabel": "総再生回数",
      "previous": "前へ",
      "next": "次へ"
    },
    "myVideos": {
      "title": "動画",
      "p1": "SNS のトレンドを取り入れながら、パークやアトラクションを題材に平均して<strong>1 日 1 本</strong>動画を投稿しています。<strong>アトラクションの POV</strong>、リアクション動画、最初の数秒で目を引く没入型の映像などを制作しています。",
      "p2": "私の編集方針は、交流を生むきっかけづくりです。パークの思い出、次の来園に向けた友達のタグ付け、来園者の行動の観察、役立つヒント、パークごとのアトラクション比較などです。",
      "gearTitle": "撮影機材",
      "gearNote": "すべてのコンテンツをこの機材で撮影しています。毎日使うメインの撮影セットです。"
    },
    "gear": {
      "harness": "チェストハーネス",
      "pole": "自撮り棒"
    },
    "contact": {
      "title": "お問い合わせ",
      "subtitle": "コラボのご相談は以下の方法でご連絡ください",
      "phoneLabel": "電話",
      "reachOut": "TikTok、メール、電話、Instagram でご連絡いただけます。"
    },
    "videos": {
      "back": "ホームに戻る",
      "parkTitle": "パーク",
      "parkLead": "このパークのすべての動画と実績。",
      "viewBoth": "TikTok + YouTube",
      "viewTikTok": "TikTok",
      "viewYouTube": "YouTube",
      "platformFilter": "プラットフォーム",
      "layoutLabel": "表示",
      "layoutStack": "リスト",
      "layoutDuo": "横並び",
      "layoutCompact": "コンパクト",
      "totalsTitle": "パークの統計",
      "totalVideos": "本の動画",
      "totalViews": "回再生",
      "totalLikes": "いいね",
      "totalComments": "件のコメント",
      "noVideos": "まだ動画はありません。",
      "noHashtag": "ハッシュタグが見つかりません",
      "linkUnavailable": "リンクは利用できません",
      "loading": "統計を読み込み中...",
      "loadError": "統計を読み込めませんでした。ページを再読み込みしてください。",
      "noParkSelected": "パークが選択されていません。",
      "comingSoon": "このパークの動画詳細は近日公開予定です。",
      "mapCta": "Google マップで開く",
      "mapTitle": "パークの地図",
      "thumbAlt": "動画のサムネイル",
      "viewsUnit": "回再生",
      "likesUnit": "いいね",
      "commentsUnit": "件のコメント"
    }
  }
};

let current = DEFAULT_LANGUAGE;
const listeners = new Set();

function lookup(dict, path) {
  return path.split('.').reduce((acc, key) => (acc && acc[key] !== undefined ? acc[key] : null), dict);
}

function readStoredLanguage() {
  try {
    return localStorage.getItem(STORAGE_KEY);
  } catch {
    return null;
  }
}

function storeLanguage(lang) {
  try {
    localStorage.setItem(STORAGE_KEY, lang);
  } catch {
    // Stockage indisponible (navigation privée) : la langue reste valable pour cette page.
  }
}

export function detectLanguage() {
  const saved = readStoredLanguage();
  if (translations[saved]) {
    return saved;
  }
  const browser = (navigator.language || '').slice(0, 2).toLowerCase();
  return translations[browser] ? browser : DEFAULT_LANGUAGE;
}

export function currentLanguage() {
  return current;
}

export function t(path) {
  const value = lookup(translations[current], path);
  return value !== null ? value : lookup(translations[DEFAULT_LANGUAGE], path) ?? '';
}

export function formatNumber(value) {
  return new Intl.NumberFormat(LANGUAGES[current].locale).format(value);
}

export function formatCompact(value) {
  const n = Number(value) || 0;
  if (n >= 1e6) {
    return `${(n / 1e6).toFixed(1).replace(/\.0$/, '')}M`;
  }
  if (n >= 1e3) {
    return `${(n / 1e3).toFixed(1).replace(/\.0$/, '')}K`;
  }
  return String(n);
}

export function onLanguageChange(callback) {
  listeners.add(callback);
}

// Traduit tous les noeuds marqués dans le DOM :
// data-i18n (texte), data-i18n-html (HTML de confiance venant de ce fichier),
// data-i18n-attr="attribut:clé;attribut:clé".
export function translateDom(root = document) {
  root.querySelectorAll('[data-i18n]').forEach((node) => {
    node.textContent = t(node.dataset.i18n);
  });
  root.querySelectorAll('[data-i18n-html]').forEach((node) => {
    node.innerHTML = t(node.dataset.i18nHtml);
  });
  root.querySelectorAll('[data-i18n-attr]').forEach((node) => {
    node.dataset.i18nAttr.split(';').forEach((pair) => {
      const [attr, key] = pair.split(':');
      node.setAttribute(attr.trim(), t(key.trim()));
    });
  });
}

export function setLanguage(lang) {
  current = translations[lang] ? lang : DEFAULT_LANGUAGE;
  storeLanguage(current);

  const page = document.body.dataset.page;
  document.documentElement.lang = current;
  document.title = t(`titles.${page}`) || document.title;
  const description = document.querySelector('meta[name="description"]');
  const metaText = lookup(translations[current], `meta.${page}`);
  if (description && metaText) {
    description.setAttribute('content', metaText);
  }

  translateDom();
  listeners.forEach((callback) => callback(current));
}
