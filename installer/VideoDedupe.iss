; Inno Setup script for the Windows installer. Built by .github/workflows/release.yml:
;   ISCC.exe /DAppVersion=1.2.0 installer\VideoDedupe.iss
; expects the published app (with ffmpeg.exe and ffprobe.exe) in artifacts\VideoDedupe.

#ifndef AppVersion
  #define AppVersion "0.0.0-dev"
#endif

[Setup]
AppId={{6F1C3E2A-8B4D-4E7A-9C5F-2D8A1B3E4F60}
AppName=VideoDedupe
AppVersion={#AppVersion}
AppPublisher=VideoDedupe
; Per user, no administrator rights: the app and its data live in the user's profile.
PrivilegesRequired=lowest
DefaultDirName={localappdata}\Programs\VideoDedupe
DisableProgramGroupPage=yes
DisableDirPage=auto
LicenseFile=..\LICENSE.txt
OutputDir=..\artifacts
OutputBaseFilename=VideoDedupe-{#AppVersion}-setup
SetupIconFile=..\src\VideoDedupe.App\Assets\app.ico
UninstallDisplayIcon={app}\VideoDedupe.App.exe
UninstallDisplayName=VideoDedupe
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
Compression=lzma2/max
SolidCompression=yes
WizardStyle=modern
CloseApplications=yes

[Languages]
Name: "german"; MessagesFile: "compiler:Languages\German.isl"
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
Source: "..\artifacts\VideoDedupe\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{autoprograms}\VideoDedupe"; Filename: "{app}\VideoDedupe.App.exe"
Name: "{autodesktop}\VideoDedupe"; Filename: "{app}\VideoDedupe.App.exe"; Tasks: desktopicon

[Run]
Filename: "{app}\VideoDedupe.App.exe"; Description: "{cm:LaunchProgram,VideoDedupe}"; Flags: nowait postinstall skipifsilent

; The database, settings and the quarantine folder stay on uninstall: they hold the user's
; decisions and possibly files moved out of the library.
