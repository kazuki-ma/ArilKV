const canvas = document.getElementById("aril-field");
const ctx = canvas.getContext("2d");
const prefersReducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)");
let particles = [];
let width = 0;
let height = 0;
let ratio = 1;
let animationFrame = 0;

function resize() {
  ratio = Math.min(window.devicePixelRatio || 1, 2);
  width = window.innerWidth;
  height = window.innerHeight;
  canvas.width = Math.floor(width * ratio);
  canvas.height = Math.floor(height * ratio);
  canvas.style.width = `${width}px`;
  canvas.style.height = `${height}px`;
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0);

  const count = Math.max(42, Math.min(96, Math.floor((width * height) / 18000)));
  particles = Array.from({ length: count }, (_, index) => {
    const lane = index / count;
    return {
      x: Math.random() * width,
      y: Math.random() * height,
      r: 1.4 + Math.random() * 4.6,
      a: 0.16 + Math.random() * 0.36,
      speed: 0.12 + Math.random() * 0.34,
      drift: -0.18 + Math.random() * 0.36,
      hue: lane < 0.58 ? "216, 51, 91" : lane < 0.82 ? "242, 166, 90" : "76, 139, 114"
    };
  });
}

function draw(time) {
  ctx.clearRect(0, 0, width, height);

  const centerX = width * 0.7;
  const centerY = height * 0.28;
  const pulse = 1 + Math.sin(time * 0.0007) * 0.04;
  const gradient = ctx.createRadialGradient(centerX, centerY, 0, centerX, centerY, Math.max(width, height) * 0.82 * pulse);
  gradient.addColorStop(0, "rgba(216, 51, 91, 0.28)");
  gradient.addColorStop(0.48, "rgba(242, 166, 90, 0.06)");
  gradient.addColorStop(1, "rgba(21, 16, 15, 0)");
  ctx.fillStyle = gradient;
  ctx.fillRect(0, 0, width, height);

  particles.forEach((particle) => {
    particle.y -= particle.speed;
    particle.x += particle.drift + Math.sin((time * 0.001 + particle.y) * 0.012) * 0.08;

    if (particle.y < -18) {
      particle.y = height + 18;
      particle.x = Math.random() * width;
    }

    if (particle.x < -18) particle.x = width + 18;
    if (particle.x > width + 18) particle.x = -18;

    ctx.beginPath();
    ctx.fillStyle = `rgba(${particle.hue}, ${particle.a})`;
    ctx.shadowColor = `rgba(${particle.hue}, 0.42)`;
    ctx.shadowBlur = 18;
    ctx.arc(particle.x, particle.y, particle.r, 0, Math.PI * 2);
    ctx.fill();
    ctx.shadowBlur = 0;
  });

  if (!prefersReducedMotion.matches) {
    animationFrame = window.requestAnimationFrame(draw);
  }
}

function start() {
  window.cancelAnimationFrame(animationFrame);
  resize();
  draw(0);
  if (!prefersReducedMotion.matches) {
    animationFrame = window.requestAnimationFrame(draw);
  }
}

window.addEventListener("resize", start);
prefersReducedMotion.addEventListener("change", start);
start();
