# OpenLatch GitHub Social Preview 生成器(1280x640)。
# 用法:python3 docs/branding/make_social_preview.py
#   -> 输出 openlatch-social-preview.png 至本脚本所在目录;上传至
#      repo Settings -> Appearance -> Social preview。
# 依赖:Pillow;优先 DejaVu(系统字体),缺失时回落 PIL 默认字体(版式基本保持)。
# 设计基线:深空渐变 #0A1424->#102136;金 #F5B23C(Leader/锁孔/底条左半);
#   青 #4CC9F0(链路/chips/眉题);口号 "Distributed locks without the zoo."
from PIL import Image, ImageDraw, ImageFont
import os

def _font(rel, size):
    for base in ("/usr/share/fonts/truetype/dejavu", "/System/Library/Fonts", "/usr/share/fonts"):
        p = os.path.join(base, rel)
        if os.path.exists(p):
            return ImageFont.truetype(p, size)
    try:
        return ImageFont.truetype(rel.replace("/usr/share/fonts/truetype/dejavu/", "DejaVu").split("/")[-1], size)
    except OSError:
        return ImageFont.load_default(size)

W, H = 1280, 640
im = Image.new("RGB", (W, H))
d = ImageDraw.Draw(im)

# ---- 背景:深空纵向渐变 + 右侧微光 ----
top, bot = (10, 20, 36), (16, 33, 54)
for y in range(H):
    t = y / H
    d.line([(0, y), (W, y)], fill=tuple(int(a + (b - a) * t) for a, b in zip(top, bot)))
glow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
gd = ImageDraw.Draw(glow)
for r, a in [(340, 26), (260, 36), (180, 48)]:
    gd.ellipse([968 - r, 290 - r, 968 + r, 290 + r], fill=(76, 201, 240, a))
im = Image.alpha_composite(im.convert("RGBA"), glow).convert("RGB")
d = ImageDraw.Draw(im)

FB = "DejaVuSans-Bold.ttf"
FR = "DejaVuSans.ttf"
FI = "DejaVuSans.ttf"

CYAN = (76, 201, 240); WHITE = (240, 246, 250); DIM = (148, 168, 188)
GOLD = (245, 178, 60); LINE = (58, 90, 120)

# ---- 右侧图形:Raft 三节点三角,顶部为 Leader(金色+锁孔) ----
nodes = [(968, 148), (842, 388), (1094, 388)]
for i in range(3):
    for j in range(i + 1, 3):
        d.line([nodes[i], nodes[j]], fill=LINE, width=3)
        mx, my = [(a + b) // 2 for a, b in zip(nodes[i], nodes[j])]
        d.ellipse([mx - 3, my - 3, mx + 3, my + 3], fill=CYAN)   # 复制链路中的消息
for k, (x, y) in enumerate(nodes):
    r = 34 if k == 0 else 26
    col = GOLD if k == 0 else (30, 58, 88)
    d.ellipse([x - r, y - r, x + r, y + r], fill=col, outline=CYAN if k else LINE, width=2)
# Leader 锁孔:圆+槽
kx, ky = nodes[0]
d.ellipse([kx - 9, ky - 12, kx + 9, ky + 6], fill=(10, 20, 36))
d.polygon([(kx - 6, ky - 1), (kx + 6, ky - 1), (kx + 3, ky + 14), (kx - 3, ky + 14)], fill=(10, 20, 36))

# ---- 左侧文字块 ----
x0 = 76
d.text((x0, 118), "D I S T R I B U T E D   L O C K   S E R V I C E", font=_font(FB, 21), fill=CYAN)
d.text((x0, 156), "OpenLatch", font=_font(FB, 92), fill=WHITE)
d.text((x0 + 4, 276), "Distributed locks without the zoo.", font=_font(FR, 31), fill=WHITE)
d.text((x0 + 4, 322), "Raft-replicated JUC primitives \xB7 embedded, zero external dependencies",
       font=_font(FR, 21), fill=DIM)

# ---- 特性 chips ----
chips = ["Java 25", "Apache Ratis", "Spring Boot 4", "TLS / mTLS"]
f_chip = _font(FB, 21)
cx = x0 + 4
for c in chips:
    w = d.textlength(c, font=f_chip)
    pad = 18
    d.rounded_rectangle([cx, 402, cx + w + 2 * pad, 452], 14, outline=CYAN, width=2)
    d.text((cx + pad, 414), c, font=f_chip, fill=WHITE)
    cx += w + 2 * pad + 14
d.text((x0 + 4, 506), "github.com/LamSpace/OpenLatch", font=_font(FR, 20), fill=DIM)

# 底条品牌色线
d.rectangle([0, H - 6, 640, H], fill=GOLD)
im.save(os.path.join(os.path.dirname(os.path.abspath(__file__)), "openlatch-social-preview.png"))
print("rendered:", im.size)
