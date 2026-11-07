// Plan 3D interactif du parc (accueil). Chaque attraction correspond à une
// section de la page : survol = mise en avant, clic = on y va. L'entrée mène
// au compte TikTok, la boutique à la chaîne YouTube.
// Three.js est chargé à la demande ; en cas d'échec, home.js affiche le plan 2D.

import * as THREE from 'https://cdn.jsdelivr.net/npm/three@0.170.0/build/three.module.min.js';
import { t } from './i18n.js';
import { ICONS, prefersReducedMotion } from './ui.js';
import { ISLAND_RADIUS, LAKE_RADIUS, RING, ZONE_LAYOUT } from './park-layout.js';

const COLORS = {
  grass: 0x93c77a,
  grassDark: 0x6fa95b,
  path: 0xecd9ae,
  water: 0x7cc4ec,
  night: 0x13233a,
  sky: 0x9bd3f2,
  skyDeep: 0x1f6fa8,
  rail: 0xd7362b,
  ticket: 0xffc93c,
  white: 0xf7f9fb,
  wood: 0x8a6a3d,
  stone: 0xd9dee5
};

// Chaque zone de la disposition partagée reçoit son attraction 3D.
const BUILDERS = {
  entrance: buildGate,
  shop: buildShop,
  about: buildFerrisWheel,
  stats: buildDropTower,
  partners: buildCastle,
  videos: buildCoaster,
  contact: buildKiosk
};
const ZONES = ZONE_LAYOUT.map((zone) => ({ ...zone, color: new THREE.Color(zone.color).getHex(), build: BUILDERS[zone.id] }));

const materialCache = new Map();
function mat(color, extra = {}) {
  const key = `${color}-${JSON.stringify(extra)}`;
  if (!materialCache.has(key)) {
    materialCache.set(key, new THREE.MeshStandardMaterial({ color, roughness: 0.85, metalness: 0, flatShading: true, ...extra }));
  }
  return materialCache.get(key);
}

function mesh(geometry, material, { x = 0, y = 0, z = 0, shadow = true } = {}) {
  const m = new THREE.Mesh(geometry, material);
  m.position.set(x, y, z);
  m.castShadow = shadow;
  m.receiveShadow = true;
  return m;
}

// Texture de rayures (toit de kiosque) ou de texte (enseigne), dessinée en canvas.
function stripeTexture(a, b, count) {
  const canvas = document.createElement('canvas');
  canvas.width = 256;
  canvas.height = 16;
  const ctx = canvas.getContext('2d');
  const w = canvas.width / count;
  for (let i = 0; i < count; i += 1) {
    ctx.fillStyle = i % 2 ? b : a;
    ctx.fillRect(i * w, 0, w + 1, canvas.height);
  }
  const texture = new THREE.CanvasTexture(canvas);
  texture.colorSpace = THREE.SRGBColorSpace;
  return texture;
}

function signTexture(text) {
  const canvas = document.createElement('canvas');
  canvas.width = 1024;
  canvas.height = 256;
  const ctx = canvas.getContext('2d');
  ctx.fillStyle = '#13233a';
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  for (let x = 40; x < canvas.width; x += 64) {
    ['#ffc93c'].forEach((color) => {
      ctx.fillStyle = color;
      ctx.beginPath();
      ctx.arc(x, 26, 9, 0, Math.PI * 2);
      ctx.arc(x, canvas.height - 26, 9, 0, Math.PI * 2);
      ctx.fill();
    });
  }
  ctx.fillStyle = '#ffc93c';
  ctx.font = '120px Bungee, Archivo, sans-serif';
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  ctx.fillText(text, canvas.width / 2, canvas.height / 2 + 8);
  const texture = new THREE.CanvasTexture(canvas);
  texture.colorSpace = THREE.SRGBColorSpace;
  texture.anisotropy = 4;
  return texture;
}

/* ---------- Attractions ---------- */

function buildDropTower(group) {
  group.add(mesh(new THREE.CylinderGeometry(1.1, 1.3, 0.8, 12), mat(COLORS.stone), { y: 0.4 }));
  for (let i = 0; i < 10; i += 1) {
    group.add(mesh(new THREE.CylinderGeometry(0.38, 0.38, 1.1, 10), mat(i % 2 ? COLORS.white : COLORS.rail), { y: 0.8 + i * 1.1 + 0.55 }));
  }
  group.add(mesh(new THREE.ConeGeometry(0.75, 1.4, 10), mat(COLORS.rail), { y: 12.6 }));
  group.add(mesh(new THREE.SphereGeometry(0.22, 8, 8), mat(COLORS.ticket, { emissive: COLORS.ticket, emissiveIntensity: 0.6 }), { y: 13.4 }));
  const ring = mesh(new THREE.TorusGeometry(0.85, 0.28, 8, 20), mat(COLORS.ticket), { y: 3 });
  ring.rotation.x = Math.PI / 2;
  group.add(ring);
  return (time) => {
    // Montée lente, chute rapide.
    const cycle = (time * 0.25) % 1;
    const h = cycle < 0.75 ? cycle / 0.75 : 1 - ((cycle - 0.75) / 0.25) ** 2;
    ring.position.y = 1.6 + h * 8.4;
  };
}

function buildFerrisWheel(group) {
  const legMat = mat(COLORS.night);
  [-1, 1].forEach((side) => {
    [-1, 1].forEach((lean) => {
      const leg = mesh(new THREE.CylinderGeometry(0.14, 0.18, 6.4, 6), legMat, { x: lean * 1.4, y: 3, z: side * 0.9 });
      leg.rotation.z = lean * -0.24;
      group.add(leg);
    });
  });
  group.add(mesh(new THREE.BoxGeometry(5, 0.4, 2.6), mat(COLORS.stone), { y: 0.2 }));

  const wheel = new THREE.Group();
  wheel.position.y = 6;
  group.add(wheel);
  [-0.55, 0.55].forEach((z) => {
    const rim = mesh(new THREE.TorusGeometry(4.2, 0.1, 6, 48), mat(COLORS.rail), { z });
    wheel.add(rim);
  });
  const hub = mesh(new THREE.CylinderGeometry(0.4, 0.4, 1.6, 10), mat(COLORS.ticket));
  hub.rotation.x = Math.PI / 2;
  wheel.add(hub);

  const gondolas = [];
  const count = 10;
  for (let i = 0; i < count; i += 1) {
    const angle = (i / count) * Math.PI * 2;
    const spoke = mesh(new THREE.BoxGeometry(0.08, 4.2, 0.08), mat(COLORS.white), { shadow: false });
    spoke.position.set(Math.cos(angle) * 2.1, Math.sin(angle) * 2.1, 0);
    spoke.rotation.z = angle - Math.PI / 2;
    wheel.add(spoke);
    const gondola = new THREE.Group();
    gondola.position.set(Math.cos(angle) * 4.2, Math.sin(angle) * 4.2, 0);
    const cabin = mesh(new THREE.CylinderGeometry(0.45, 0.38, 0.6, 8), mat(i % 2 ? COLORS.sky : COLORS.ticket), { y: -0.55 });
    const roof = mesh(new THREE.ConeGeometry(0.5, 0.3, 8), mat(COLORS.rail), { y: -0.1 });
    gondola.add(cabin, roof);
    wheel.add(gondola);
    gondolas.push(gondola);
  }
  return (time) => {
    wheel.rotation.z = time * 0.18;
    gondolas.forEach((g) => { g.rotation.z = -wheel.rotation.z; });
  };
}

function buildCastle(group) {
  group.add(mesh(new THREE.BoxGeometry(4.6, 2.4, 3.4), mat(COLORS.white), { y: 1.2 }));
  group.add(mesh(new THREE.BoxGeometry(2.2, 4.2, 2), mat(COLORS.white), { y: 2.1 }));
  group.add(mesh(new THREE.ConeGeometry(1.6, 2.2, 4), mat(COLORS.skyDeep), { y: 5.3 }));
  group.add(mesh(new THREE.BoxGeometry(1, 1.4, 0.1), mat(COLORS.night), { y: 0.7, z: 1.72 }));
  const flags = [COLORS.rail, COLORS.ticket, COLORS.sky, COLORS.skyDeep];
  [[-2.3, -1.7], [2.3, -1.7], [-2.3, 1.7], [2.3, 1.7]].forEach(([x, z], i) => {
    group.add(mesh(new THREE.CylinderGeometry(0.6, 0.65, 3.4, 10), mat(COLORS.white), { x, y: 1.7, z }));
    group.add(mesh(new THREE.ConeGeometry(0.8, 1.6, 10), mat(COLORS.rail), { x, y: 4.2, z }));
    group.add(mesh(new THREE.CylinderGeometry(0.03, 0.03, 1, 4), mat(COLORS.night), { x, y: 5.4, z, shadow: false }));
    const flag = mesh(new THREE.BoxGeometry(0.6, 0.36, 0.03), mat(flags[i]), { x: x + 0.3, y: 5.7, z, shadow: false });
    group.add(flag);
  });
  return null;
}

function buildCoaster(group) {
  const points = [
    [-5, 1.2, -3.5], [-1, 6.5, -4], [3, 1.4, -3.6], [5.4, 3.4, -0.6],
    [4, 1.2, 3], [0, 4.2, 4], [-3.6, 1.6, 3.2], [-5.6, 2.6, 0]
  ].map(([x, y, z]) => new THREE.Vector3(x, y, z));
  const curve = new THREE.CatmullRomCurve3(points, true, 'centripetal');
  group.add(mesh(new THREE.TubeGeometry(curve, 240, 0.16, 6, true), mat(COLORS.rail)));

  const supportMat = mat(COLORS.white);
  for (let i = 0; i < 24; i += 1) {
    const p = curve.getPointAt(i / 24);
    group.add(mesh(new THREE.CylinderGeometry(0.07, 0.09, p.y, 5), supportMat, { x: p.x, y: p.y / 2, z: p.z }));
  }
  group.add(mesh(new THREE.BoxGeometry(2.6, 1.2, 1.6), mat(COLORS.ticket), { x: -5.4, y: 0.6, z: 0 }));
  group.add(mesh(new THREE.BoxGeometry(3, 0.25, 2), mat(COLORS.rail), { x: -5.4, y: 1.35, z: 0 }));

  const cars = [0, 1, 2].map((i) => {
    const car = new THREE.Group();
    car.add(mesh(new THREE.BoxGeometry(0.9, 0.45, 0.62), mat(i === 0 ? COLORS.sky : COLORS.ticket), { y: 0.32 }));
    car.add(mesh(new THREE.SphereGeometry(0.15, 6, 6), mat(COLORS.night), { y: 0.68, x: -0.1 }));
    group.add(car);
    return car;
  });
  const tangent = new THREE.Vector3();
  const ahead = new THREE.Vector3();
  return (time) => {
    cars.forEach((car, i) => {
      const u = ((time * 0.07) - i * 0.016 + 10) % 1;
      car.position.copy(curve.getPointAt(u));
      curve.getTangentAt(u, tangent);
      ahead.copy(car.position).add(tangent);
      car.lookAt(ahead);
      car.rotateY(-Math.PI / 2);
    });
  };
}

function buildKiosk(group) {
  group.add(mesh(new THREE.CylinderGeometry(1.7, 1.8, 1.8, 16), mat(COLORS.white), { y: 0.9 }));
  group.add(mesh(new THREE.CylinderGeometry(1.72, 1.72, 0.5, 16), mat(COLORS.night), { y: 1.55 }));
  const roofMat = new THREE.MeshStandardMaterial({ map: stripeTexture('#d7362b', '#f7f9fb', 16), roughness: 0.8, flatShading: true });
  group.add(mesh(new THREE.ConeGeometry(2.6, 1.8, 16), roofMat, { y: 2.9 }));
  group.add(mesh(new THREE.CylinderGeometry(0.04, 0.04, 1.2, 4), mat(COLORS.night), { y: 4.2, shadow: false }));
  const flag = mesh(new THREE.BoxGeometry(0.7, 0.4, 0.03), mat(COLORS.ticket), { x: 0.35, y: 4.6, shadow: false });
  group.add(flag);
  // Petit panneau « i ».
  group.add(mesh(new THREE.CylinderGeometry(0.06, 0.06, 1.6, 4), mat(COLORS.night), { x: 2.4, y: 0.8, z: 1 }));
  group.add(mesh(new THREE.CylinderGeometry(0.5, 0.5, 0.1, 16), mat(COLORS.skyDeep), { x: 2.4, y: 1.8, z: 1 }).rotateX(Math.PI / 2));
  return (time) => {
    flag.rotation.y = Math.sin(time * 3) * 0.3;
  };
}

/* ---------- Décor ---------- */

function buildGround(scene) {
  const island = mesh(new THREE.CylinderGeometry(ISLAND_RADIUS, ISLAND_RADIUS + 1.5, 2, 64), mat(COLORS.grass), { y: -1, shadow: false });
  scene.add(island);
  const rim = mesh(new THREE.CylinderGeometry(ISLAND_RADIUS + 1.6, ISLAND_RADIUS + 1.6, 0.6, 64), mat(COLORS.grassDark), { y: -1.8, shadow: false });
  scene.add(rim);

  // Allée circulaire + allées vers chaque attraction et vers l'entrée.
  const ring = mesh(new THREE.RingGeometry(RING.inner, RING.outer, 64), mat(COLORS.path), { y: 0.02, shadow: false });
  ring.rotation.x = -Math.PI / 2;
  scene.add(ring);
  const spokes = ZONES.map((z) => z.pos);
  spokes.forEach(([x, , z]) => {
    const length = Math.hypot(x, z) - 9;
    const angle = Math.atan2(x, z);
    const alley = mesh(new THREE.PlaneGeometry(1.8, length), mat(COLORS.path), { y: 0.025, shadow: false });
    alley.rotation.x = -Math.PI / 2;
    alley.rotation.z = angle;
    alley.position.set(Math.sin(angle) * (9 + length / 2), 0.025, Math.cos(angle) * (9 + length / 2));
    scene.add(alley);
  });

  // Lac central avec fontaine.
  const lake = mesh(new THREE.CircleGeometry(LAKE_RADIUS, 48), mat(COLORS.water, { roughness: 0.3 }), { y: 0.04, shadow: false });
  lake.rotation.x = -Math.PI / 2;
  scene.add(lake);
  scene.add(mesh(new THREE.CylinderGeometry(1.2, 1.4, 0.5, 16), mat(COLORS.stone), { y: 0.25 }));
  const jet = mesh(new THREE.ConeGeometry(0.5, 2.4, 10), mat(COLORS.white, { transparent: true, opacity: 0.75 }), { y: 1.6, shadow: false });
  scene.add(jet);
  return jet;
}

function buildGate(group) {
  [-3, 3].forEach((x) => {
    group.add(mesh(new THREE.BoxGeometry(1, 4.4, 1), mat(COLORS.rail), { x, y: 2.2 }));
    group.add(mesh(new THREE.ConeGeometry(0.8, 1, 4), mat(COLORS.ticket), { x, y: 4.9 }));
  });
  const signMat = new THREE.MeshStandardMaterial({ map: signTexture('CLIP2EP'), roughness: 0.6 });
  const side = mat(COLORS.night);
  group.add(mesh(new THREE.BoxGeometry(7.6, 1.9, 0.4), [side, side, side, side, signMat, side], { y: 4.3 }));
  // L'enseigne est redessinée une fois la police Bungee chargée.
  document.fonts?.load('120px Bungee').then(() => {
    signMat.map.dispose();
    signMat.map = signTexture('CLIP2EP');
    signMat.needsUpdate = true;
  }).catch(() => {});
  return null;
}

function youtubeTexture() {
  const canvas = document.createElement('canvas');
  canvas.width = 256;
  canvas.height = 180;
  const ctx = canvas.getContext('2d');
  ctx.fillStyle = '#d7362b';
  ctx.beginPath();
  ctx.roundRect(8, 8, 240, 164, 46);
  ctx.fill();
  ctx.fillStyle = '#ffffff';
  ctx.beginPath();
  ctx.moveTo(100, 52);
  ctx.lineTo(176, 90);
  ctx.lineTo(100, 128);
  ctx.closePath();
  ctx.fill();
  const texture = new THREE.CanvasTexture(canvas);
  texture.colorSpace = THREE.SRGBColorSpace;
  return texture;
}

function buildShop(group) {
  group.rotation.y = 0.45;
  group.add(mesh(new THREE.BoxGeometry(4.4, 2.8, 3.2), mat(COLORS.white), { y: 1.4 }));
  group.add(mesh(new THREE.BoxGeometry(4.8, 0.35, 3.6), mat(COLORS.night), { y: 2.95 }));
  group.add(mesh(new THREE.BoxGeometry(2.6, 1.3, 0.1), mat(COLORS.skyDeep), { x: -0.5, y: 1.4, z: 1.62 }));
  group.add(mesh(new THREE.BoxGeometry(0.9, 1.9, 0.1), mat(COLORS.wood), { x: 1.5, y: 0.95, z: 1.62 }));
  const awningMat = new THREE.MeshStandardMaterial({ map: stripeTexture('#d7362b', '#f7f9fb', 10), roughness: 0.8, flatShading: true });
  const awning = mesh(new THREE.BoxGeometry(4.6, 0.12, 1.2), awningMat, { y: 2.45, z: 2.05 });
  awning.rotation.x = 0.35;
  group.add(awning);
  // Enseigne « lecture » sur le toit.
  const logoMat = new THREE.MeshStandardMaterial({ map: youtubeTexture(), roughness: 0.5 });
  const side = mat(COLORS.rail);
  group.add(mesh(new THREE.BoxGeometry(2.4, 1.7, 0.25), [side, side, side, side, logoMat, side], { y: 4.1 }));
  group.add(mesh(new THREE.BoxGeometry(0.15, 0.6, 0.15), mat(COLORS.night), { y: 3.3 }));
  return null;
}

// Lampadaires allumés le long de l'allée circulaire (lumière émissive : aucun coût d'éclairage).
function buildLamps(scene) {
  const count = 14;
  const poles = new THREE.InstancedMesh(new THREE.CylinderGeometry(0.06, 0.08, 1.8, 5), mat(COLORS.night), count);
  const bulbs = new THREE.InstancedMesh(
    new THREE.SphereGeometry(0.26, 8, 6),
    new THREE.MeshBasicMaterial({ color: 0xffe08a }),
    count
  );
  const matrix = new THREE.Matrix4();
  for (let i = 0; i < count; i += 1) {
    const angle = (i / count) * Math.PI * 2 + 0.22;
    const x = Math.cos(angle) * (RING.outer + 0.5);
    const z = Math.sin(angle) * (RING.outer + 0.5);
    matrix.makeTranslation(x, 0.9, z);
    poles.setMatrixAt(i, matrix);
    matrix.makeTranslation(x, 1.9, z);
    bulbs.setMatrixAt(i, matrix);
  }
  poles.castShadow = true;
  scene.add(poles, bulbs);
}

function buildTrees(scene) {
  const spots = [];
  let seed = 7;
  const random = () => {
    seed = (seed * 16807) % 2147483647;
    return seed / 2147483647;
  };
  while (spots.length < 46) {
    const angle = random() * Math.PI * 2;
    const radius = 15 + random() * 8;
    const x = Math.cos(angle) * radius;
    const z = Math.sin(angle) * radius;
    const clear = ZONES.every(({ pos }) => Math.hypot(pos[0] - x, pos[2] - z) > 6.5) && Math.abs(x) + 0.2 * z > 2.2;
    if (clear) {
      spots.push([x, z, 0.7 + random() * 0.7]);
    }
  }
  const foliage = new THREE.InstancedMesh(new THREE.ConeGeometry(1, 2.4, 7), mat(COLORS.grassDark), spots.length);
  const trunks = new THREE.InstancedMesh(new THREE.CylinderGeometry(0.16, 0.2, 0.8, 5), mat(COLORS.wood), spots.length);
  const m = new THREE.Matrix4();
  spots.forEach(([x, z, s], i) => {
    m.compose(new THREE.Vector3(x, 0.4 * s + 1.2 * s, z), new THREE.Quaternion(), new THREE.Vector3(s, s, s));
    foliage.setMatrixAt(i, m);
    m.compose(new THREE.Vector3(x, 0.4 * s, z), new THREE.Quaternion(), new THREE.Vector3(s, s, s));
    trunks.setMatrixAt(i, m);
  });
  foliage.castShadow = true;
  trunks.castShadow = true;
  scene.add(foliage, trunks);
}

/* ---------- Scène ---------- */

export async function initPark3d(root) {
  if (!root) {
    throw new Error('Conteneur du plan introuvable');
  }
  const reduced = prefersReducedMotion();
  // Téléphones et tablettes : définition, ombres et cadence réduites pour garder un rendu fluide.
  const compact = window.matchMedia('(pointer: coarse)').matches || Math.min(window.innerWidth, window.innerHeight) < 700;
  const labelsLayer = root.querySelector('.park3d-labels');

  // Un seul contexte WebGL : s'il ne peut pas être créé, l'erreur fait afficher le plan 2D.
  // Pas d'anticrénelage sur mobile : coûteux, et peu visible sur ces écrans très denses.
  const renderer = new THREE.WebGLRenderer({ antialias: !compact, alpha: true, powerPreference: 'high-performance' });
  renderer.setPixelRatio(Math.min(window.devicePixelRatio, compact ? 1.5 : 2));
  renderer.shadowMap.enabled = true;
  renderer.shadowMap.type = THREE.PCFSoftShadowMap;
  renderer.outputColorSpace = THREE.SRGBColorSpace;
  const canvas = renderer.domElement;
  canvas.className = 'park3d-canvas';
  canvas.setAttribute('role', 'img');
  root.prepend(canvas);

  const scene = new THREE.Scene();
  // Ambiance de nuit : le brouillard se fond dans le ciel bleu nuit de la page.
  scene.fog = new THREE.Fog(0x13233a, 70, 140);
  const camera = new THREE.PerspectiveCamera(32, 1, 1, 220);

  scene.add(new THREE.HemisphereLight(0x9db8e6, 0x1c2b22, 1.25));
  // Clair de lune (côté de la lune dessinée dans le ciel) + lumière chaude des allées.
  const sun = new THREE.DirectionalLight(0xcfe0ff, 1.9);
  sun.position.set(20, 30, -12);
  const parkGlow = new THREE.DirectionalLight(0xffd59a, 0.7);
  parkGlow.position.set(-10, 14, 22);
  scene.add(parkGlow);
  sun.castShadow = true;
  sun.shadow.mapSize.set(compact ? 512 : 1024, compact ? 512 : 1024);
  Object.assign(sun.shadow.camera, { left: -28, right: 28, top: 28, bottom: -28, near: 1, far: 80 });
  sun.shadow.bias = -0.0006;
  scene.add(sun);

  // Tout le parc dans un groupe, pour l'animation d'ouverture (l'île monte en tournant).
  const world = new THREE.Group();
  scene.add(world);
  const jet = buildGround(world);
  buildTrees(world);
  buildLamps(world);

  // Attractions + zones cliquables (un cylindre invisible facilite la visée).
  const hitMaterial = new THREE.MeshBasicMaterial({ visible: false });
  const zones = ZONES.map((zone, index) => {
    const group = new THREE.Group();
    group.position.set(...zone.pos);
    const plinthMaterial = mat(zone.color, { emissive: 0x000000 }).clone();
    const plinth = mesh(new THREE.CylinderGeometry(4.2, 4.4, 0.3, 32), plinthMaterial, { y: 0.15 });
    group.add(plinth);
    const ride = new THREE.Group();
    group.add(ride);
    const animate = zone.build(ride);
    const box = new THREE.Box3().setFromObject(ride);
    const height = Math.max(4, box.max.y);
    const hit = new THREE.Mesh(new THREE.CylinderGeometry(4.6, 4.6, height, 12), hitMaterial);
    hit.position.y = height / 2;
    hit.userData.zone = zone.id;
    group.add(hit);
    world.add(group);

    const label = document.createElement('a');
    label.className = zone.link ? `park3d-label park3d-label--${zone.link.icon}` : 'park3d-label';
    label.dataset.zone = zone.id;
    label.style.setProperty('--i', index);
    if (zone.link) {
      label.href = zone.link.href;
      label.target = '_blank';
      label.rel = 'noopener noreferrer';
    } else {
      label.href = `#${zone.id}`;
    }
    label.innerHTML = `<b>${zone.link ? ICONS[zone.link.icon] : zone.station}</b><span class="park3d-label-name"></span>`;
    labelsLayer.append(label);

    return { ...zone, group, ride, plinthMaterial, hit, animate, anchor: new THREE.Vector3(zone.pos[0], Math.min(height, 6.5) + 1, zone.pos[2]), label, scale: 1 };
  });
  const hits = zones.map((z) => z.hit);

  const relabel = () => {
    canvas.setAttribute('aria-label', t('park.mapAria'));
    zones.forEach((zone) => {
      zone.label.querySelector('.park3d-label-name').textContent = zone.link ? zone.link.label : t(`nav.${zone.id}`);
    });
    measureLabels();
    requestRender();
  };


  /* --- Caméra : orbite douce, glisser pour tourner, parallaxe au pointeur --- */
  const POLAR_MIN = 0.5;
  const POLAR_MAX = 1.22;
  // En portrait, une vue plus plongeante remplit mieux l'écran.
  const startPolar = root.clientWidth < root.clientHeight * 0.85 ? 0.74 : 0.9;
  const view = { azimuth: -0.35, polar: startPolar, target: -0.35, polarTarget: startPolar, pointerX: 0, pointerY: 0, px: 0, py: 0 };
  let radius = 60;
  let shift = 0;
  let width = 1;
  let height = 1;
  const center = new THREE.Vector3(0, 1.5, 1);

  const resize = () => {
    width = root.clientWidth;
    height = root.clientHeight;
    if (!width || !height) {
      return;
    }
    renderer.setSize(width, height, false);
    camera.aspect = width / height;
    // En portrait, un angle plus large évite de reculer la caméra trop loin.
    camera.fov = camera.aspect < 0.85 ? 44 : 32;
    const vfov = THREE.MathUtils.degToRad(camera.fov);
    const hfov = 2 * Math.atan(Math.tan(vfov / 2) * camera.aspect);
    // Demi-largeur de parc à faire tenir : un peu plus de marge sur les écrans presque carrés.
    const fit = camera.aspect < 0.75 ? 24 : camera.aspect < 1.4 ? 31 : 29;
    radius = Math.max(fit / Math.tan(hfov / 2), 24 / Math.tan(vfov / 2));
    shift = 0;
    camera.far = radius * 3;
    camera.updateProjectionMatrix();
    scene.fog.near = radius * 1.2;
    scene.fog.far = radius * 2.6;
    measureLabels();
    requestRender();
  };

  const right = new THREE.Vector3();
  const look = new THREE.Vector3();
  const placeCamera = () => {
    const az = view.azimuth + view.px * 0.12;
    const polar = view.polar + view.py * 0.05;
    camera.position.set(
      Math.sin(az) * Math.sin(polar) * radius,
      Math.cos(polar) * radius,
      Math.cos(az) * Math.sin(polar) * radius
    );
    right.set(Math.cos(az), 0, -Math.sin(az));
    look.copy(center).addScaledVector(right, -shift);
    camera.position.addScaledVector(right, -shift);
    camera.lookAt(look);
  };

  /* --- Survol et clic --- */
  const raycaster = new THREE.Raycaster();
  const pointer = new THREE.Vector2();
  let hovered = null;
  let dragging = null;

  const setHovered = (id) => {
    if (hovered === id) {
      return;
    }
    hovered = id;
    canvas.style.cursor = id ? 'pointer' : dragging ? 'grabbing' : 'grab';
    zones.forEach((zone) => zone.label.classList.toggle('is-active', zone.id === id));
    requestRender();
  };

  const pick = (event) => {
    const rect = canvas.getBoundingClientRect();
    pointer.set(((event.clientX - rect.left) / rect.width) * 2 - 1, -((event.clientY - rect.top) / rect.height) * 2 + 1);
    raycaster.setFromCamera(pointer, camera);
    const hit = raycaster.intersectObjects(hits, false)[0];
    return hit ? hit.object.userData.zone : null;
  };

  const goTo = (id) => {
    const zone = zones.find((z) => z.id === id);
    if (zone?.link) {
      window.open(zone.link.href, '_blank', 'noopener');
      return;
    }
    document.getElementById(id)?.scrollIntoView({ behavior: reduced ? 'auto' : 'smooth' });
  };

  canvas.addEventListener('pointermove', (event) => {
    const rect = canvas.getBoundingClientRect();
    view.pointerX = ((event.clientX - rect.left) / rect.width) * 2 - 1;
    view.pointerY = ((event.clientY - rect.top) / rect.height) * 2 - 1;
    if (dragging) {
      const dx = event.clientX - dragging.x;
      const dy = event.clientY - dragging.y;
      // Un doigt bouge toujours un peu : plus de tolérance au toucher qu'à la souris.
      const slop = dragging.tilt ? 4 : 12;
      if (Math.abs(dx) > slop || Math.abs(dy) > slop) {
        dragging.moved = true;
      }
      view.target = dragging.start - dx * 0.006;
      // À la souris, glisser verticalement incline la vue (sur mobile, le glisser vertical fait défiler la page).
      if (dragging.tilt) {
        view.polarTarget = THREE.MathUtils.clamp(dragging.startPolar - dy * 0.004, POLAR_MIN, POLAR_MAX);
      }
      requestRender();
      return;
    }
    if (event.pointerType === 'mouse') {
      setHovered(pick(event));
    }
    requestRender();
  });
  canvas.addEventListener('pointerleave', () => {
    view.pointerX = 0;
    view.pointerY = 0;
    setHovered(null);
  });
  canvas.addEventListener('pointerdown', (event) => {
    dragging = { x: event.clientX, y: event.clientY, start: view.target, startPolar: view.polarTarget, tilt: event.pointerType === 'mouse', moved: false };
  });
  const endDrag = (event) => {
    if (!dragging) {
      return;
    }
    const { moved } = dragging;
    dragging = null;
    if (!moved) {
      const id = pick(event);
      if (id) {
        goTo(id);
      }
    }
    canvas.style.cursor = hovered ? 'pointer' : 'grab';
  };
  canvas.addEventListener('pointerup', endDrag);
  canvas.addEventListener('pointercancel', () => { dragging = null; });

  zones.forEach((zone) => {
    zone.label.addEventListener('mouseenter', () => setHovered(zone.id));
    zone.label.addEventListener('mouseleave', () => setHovered(null));
    zone.label.addEventListener('focus', () => setHovered(zone.id));
    zone.label.addEventListener('blur', () => setHovered(null));
  });


  /* --- Ouverture du parc : l'île monte en tournant, puis les attractions surgissent une à une --- */
  const INTRO_ISLAND = 0.9;
  const INTRO_DELAY = 0.4;
  const INTRO_STAGGER = 0.12;
  const INTRO_POP = 0.55;
  const INTRO_END = INTRO_DELAY + (zones.length - 1) * INTRO_STAGGER + INTRO_POP;
  let intro = reduced ? INTRO_END : 0;
  zones.forEach((zone) => {
    zone.labelPop = reduced ? 1 : 0;
    zone.label.style.opacity = reduced ? '' : '0';
  });
  let opened = false;
  const clamp01 = (value) => Math.min(1, Math.max(0, value));
  const easeOutCubic = (x) => 1 - (1 - x) ** 3;
  const easeOutBack = (x) => 1 + 2.2 * (x - 1) ** 3 + 1.2 * (x - 1) ** 2;
  if (!reduced) {
    view.azimuth -= 0.9;
  }

  // Calée sur l'horloge réelle : même durée quel que soit le nombre d'images par seconde.
  let introStart = 0;
  function stepIntro(now) {
    if (opened) {
      return;
    }
    introStart = introStart || now;
    intro = (now - introStart) / 1000;
    const rise = easeOutCubic(clamp01(intro / INTRO_ISLAND));
    world.position.y = -14 * (1 - rise);
    world.rotation.y = -0.8 * (1 - rise);
    world.updateMatrixWorld();
    zones.forEach((zone, index) => {
      const progress = clamp01((intro - INTRO_DELAY - index * INTRO_STAGGER) / INTRO_POP);
      const pop = easeOutBack(progress);
      zone.group.scale.setScalar(Math.max(pop, 0.001));
      // Le panneau apparaît en même temps que son attraction, juste au-dessus.
      zone.labelPop = progress;
      zone.label.style.opacity = progress.toFixed(3);
    });
    if (intro >= INTRO_END) {
      opened = true;
      world.position.y = 0;
      world.rotation.y = 0;
      zones.forEach((zone) => {
        zone.group.scale.setScalar(1);
        zone.labelPop = 1;
        zone.label.style.opacity = '';
      });
      world.updateMatrixWorld();
      root.classList.add('is-open');
    }
  }

  /* --- Boucle de rendu (en pause hors écran) --- */
  const clock = new THREE.Clock();
  let elapsed = 0;
  let visible = true;
  let frame = 0;
  const projected = new THREE.Vector3();

  let lastFrame = 0;
  function renderFrame(now = performance.now()) {
    frame = 0;
    if (compact && !dragging && now - lastFrame < 30) {
      frame = requestAnimationFrame(renderFrame);
      return;
    }
    lastFrame = now;
    const dt = Math.min(clock.getDelta(), 0.05);
    stepIntro(now);
    if (!reduced) {
      elapsed += dt;
      if (!dragging && !hovered) {
        view.target += dt * 0.035;
      }
    }

    view.azimuth += (view.target - view.azimuth) * (reduced ? 1 : 0.06);
    view.polar += (view.polarTarget - view.polar) * (reduced ? 1 : 0.08);
    view.px += (view.pointerX - view.px) * 0.05;
    view.py += (view.pointerY - view.py) * 0.05;
    placeCamera();

    zones.forEach((zone) => {
      if (zone.animate) {
        zone.animate(elapsed);
      }
      const active = zone.id === hovered;
      zone.scale += ((active ? 1.08 : 1) - zone.scale) * (reduced ? 1 : 0.15);
      zone.ride.scale.setScalar(zone.scale);
      zone.plinthMaterial.emissive.setHex(active ? 0x6b4a00 : 0x000000);

      projected.copy(zone.anchor).applyMatrix4(world.matrixWorld).project(camera);
      zone.screen = {
        x: (projected.x * 0.5 + 0.5) * width,
        y: (-projected.y * 0.5 + 0.5) * height,
        w: zone.labelSize.w,
        h: zone.labelSize.h
      };
    });
    placeLabels();
    jet.scale.y = 1 + Math.sin(elapsed * 4) * 0.12;

    renderer.render(scene, camera);

    const settling = Math.abs(view.target - view.azimuth) > 0.0005 || Math.abs(view.polarTarget - view.polar) > 0.0005 || Math.abs(view.pointerX - view.px) > 0.001;
    if (visible && (!reduced || settling || !opened)) {
      frame = requestAnimationFrame(renderFrame);
    }
  }

  // Taille des étiquettes, mesurée seulement quand elle peut changer
  // (langue, taille d'écran) : la lire à chaque image forcerait un recalcul de mise en page.
  function measureLabels() {
    zones.forEach((zone) => {
      zone.labelSize = { w: zone.label.offsetWidth, h: zone.label.offsetHeight };
    });
  }

  // Étiquettes : on écarte verticalement celles qui se chevauchent.
  function placeLabels() {
    const sorted = [...zones].sort((a, b) => b.screen.y - a.screen.y);
    sorted.forEach((zone, i) => {
      for (let j = 0; j < i; j += 1) {
        const other = sorted[j].screen;
        const overlapX = Math.abs(zone.screen.x - other.x) < (zone.screen.w + other.w) / 2 + 6;
        const overlapY = zone.screen.y > other.y - other.h - 6;
        if (overlapX && overlapY) {
          zone.screen.y = other.y - other.h - 6;
        }
      }
    });
    zones.forEach(({ label, screen, labelPop = 1 }) => {
      const scale = 0.6 + 0.4 * labelPop;
      label.style.transform = `translate(${screen.x.toFixed(1)}px, ${screen.y.toFixed(1)}px) translate(-50%, -100%) scale(${scale.toFixed(3)})`;
    });
  }

  function requestRender() {
    if (!frame && visible) {
      frame = requestAnimationFrame(renderFrame);
    }
  }

  new ResizeObserver(resize).observe(root);
  new IntersectionObserver(([entry]) => {
    visible = entry.isIntersecting && !document.hidden;
    if (visible) {
      clock.getDelta();
      requestRender();
    }
  }).observe(root);
  document.addEventListener('visibilitychange', () => {
    visible = !document.hidden;
    if (visible) {
      clock.getDelta();
      requestRender();
    }
  });

  relabel();
  document.fonts?.ready.then(() => measureLabels());
  resize();
  root.classList.add('is-ready');
  requestRender();

  return { relabel };
}
