// Toutes les traductions du site (accueil + page parc) et la logique de langue.

export const LANGUAGES = {
  fr: { label: 'Français', flag: 'fr', locale: 'fr-FR' },
  en: { label: 'English', flag: 'gb', locale: 'en-US' },
  es: { label: 'Español', flag: 'es', locale: 'es-ES' },
  de: { label: 'Deutsch', flag: 'de', locale: 'de-DE' },
  nl: { label: 'Nederlands', flag: 'nl', locale: 'nl-NL' }
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
