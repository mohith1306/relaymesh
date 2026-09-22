# RelayMesh Cloud Deployment Guide

## Option 1: Render (Free - Recommended)

1. Push your code to GitHub:
```bash
git add .
git commit -m "Add relay server"
git push
```

2. Go to https://render.com and sign up (free)

3. Click "New" → "Web Service"

4. Connect your GitHub repo

5. Settings:
   - Name: relaymesh-relay
   - Runtime: Docker
   - Port: 8080
   - Instance Type: Free

6. Click "Create Web Service"

7. Your relay server will be at: `https://relaymesh-relay.onrender.com`

## Option 2: Fly.io (Free)

1. Install Fly CLI:
```bash
curl -L https://fly.io/install.sh | sh
fly auth signup
```

2. Initialize and deploy:
```bash
fly launch
fly deploy
```

3. Your relay server will be at: `https://your-app-name.fly.dev`

## Option 3: Railway ($5 Free Credit)

1. Install Railway CLI:
```bash
npm i -g @railway/cli
railway login
```

2. Deploy:
```bash
railway init
railway up
```

3. Your relay server will be at: `https://your-app-name.up.railway.app`

---

## After Deployment

1. Update the relay server URL in your app
2. Share the public URL with anyone
3. Devices can connect from ANY network (mobile data, different WiFi, etc.)
