# -*- mode: python ; coding: utf-8 -*-

block_cipher = None


a = Analysis(['src\\scrapy_wrapper.py'],
             pathex=['.'],
             binaries=[],
             datas=[('venv\\Lib\\site-packages\\langdetect\\utils\\messages.properties', 'langdetect\\utils'),
					('venv\\Lib\\site-packages\\langdetect\\profiles', 'langdetect\\profiles'),
					('src\\parsers.py', '.'),
					('src\\pipelines.py', '.'),
					('src\\textract_pdf', 'textract_pdf')],
			 hiddenimports=['chardet'],
             hookspath=['src\\hooks'],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          [],
          exclude_binaries=True,
          name='OWS-scrapy-wrapper',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          console=True )
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=False,
               upx=True,
               upx_exclude=[],
               name='OWS-scrapy-wrapper')
