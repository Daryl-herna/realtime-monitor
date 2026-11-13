#!/usr/bin/env python
"""一键启动：python run.py"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.main import main

if __name__ == "__main__":
    sys.exit(main())
