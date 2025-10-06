#!/bin/bash

# 1. برو به فولدر پروژه (در صورت نیاز)
# cd /path/to/your/local-repo

# 2. پاک کردن فایل‌های .DS_Store
# find . -name .DS_Store -delete

# 3. اضافه کردن تغییرات به staging
git add .

# 4. Commit تغییرات لوکال
git commit -m "Auto commit from script"

# 5. Pull آخرین تغییرات از GitHub و Merge
# git pull origin main --rebase
# git pull origin main

# 6. Push تغییرات به GitHub
git push origin main
