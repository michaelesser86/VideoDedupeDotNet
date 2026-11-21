using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;

namespace VideoDedupe.App.Services;

/// <summary>
/// UI texts in English and German, switchable at runtime. The English text is the key, so code
/// and XAML stay readable; a text without translation falls back to English.
/// </summary>
public sealed class Loc : INotifyPropertyChanged
{
    public static Loc Instance { get; } = new();

    public const string English = "en", German = "de";

    private string _language = CultureInfo.CurrentUICulture.TwoLetterISOLanguageName == German ? German : English;

    public event PropertyChangedEventHandler? PropertyChanged;

    /// <summary>Raised after the language changed, for texts computed in code.</summary>
    public event Action? LanguageChanged;

    public string Language
    {
        get => _language;
        set
        {
            var language = value == German ? German : English;
            if (language == _language) return;
            _language = language;
            Version++;
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Language)));
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Version)));
            LanguageChanged?.Invoke();
        }
    }

    /// <summary>Changes with every language switch; XAML texts bind to it to refresh.</summary>
    public int Version { get; private set; }

    public string Translate(string english) =>
        _language == German && GermanTexts.TryGetValue(english, out var german) ? german : english;

    /// <summary>Translated text.</summary>
    public static string T(string english) => Instance.Translate(english);

    /// <summary>Translated format string, filled with <paramref name="args"/>.</summary>
    public static string F(string englishFormat, params object?[] args) =>
        string.Format(CultureInfo.CurrentCulture, T(englishFormat), args);

    internal static readonly Dictionary<string, string> GermanTexts = new()
    {
        // Header and progress
        ["Find and clean up duplicate videos"] = "Doppelte Videos finden und aufräumen",
        ["Scan library"] = "Bibliothek scannen",
        ["Read new and changed videos in the enabled folders (step 1)."] = "Neue und geänderte Videos in den aktiven Ordnern einlesen (Schritt 1).",
        ["Find duplicates"] = "Duplikate finden",
        ["Compare the scanned videos and group the copies (step 2)."] = "Gescannte Videos vergleichen und Kopien gruppieren (Schritt 2).",
        ["Cancel"] = "Abbrechen",
        ["Scanning library · {0} of {1} videos · {2} unreadable"] = "Bibliothek wird gescannt · {0} von {1} Videos · {2} nicht lesbar",
        ["Finding duplicates · {0} · {1} of {2} videos"] = "Duplikatsuche · {0} · {1} von {2} Videos",
        ["Cleaning up… (see the status above)"] = "Wird aufgeräumt … (siehe Status oben)",
        ["Undo (Ctrl+Z)"] = "Rückgängig (Strg+Z)",
        ["Estimating remaining time..."] = "Restzeit wird geschätzt …",
        ["Approx. {0} remaining ({1} elapsed)"] = "Noch ca. {0} ({1} vergangen)",
        ["Completed in {0}"] = "Fertig in {0}",
        ["Starting..."] = "Startet …",
        ["Clearing existing groups"] = "Alte Gruppen werden entfernt",
        ["Loading candidates"] = "Kandidaten werden geladen",
        ["Hashing frames"] = "Bilder werden analysiert",
        ["Comparing duration ranges"] = "Längenbereiche werden verglichen",
        ["Refining borderline matches"] = "Knappe Treffer werden geprüft",
        ["Searching shortened copies"] = "Gekürzte Kopien werden gesucht",
        ["Checking shortened copies"] = "Gekürzte Kopien werden geprüft",
        ["Saved groups"] = "Gruppen gespeichert",
        ["Done"] = "Fertig",

        // Tabs and group list
        ["Duplicates ({0})"] = "Duplikate ({0})",
        ["Folders ({0})"] = "Ordner ({0})",
        ["Settings"] = "Einstellungen",
        ["Search (Ctrl+F)"] = "Suchen (Strg+F)",
        ["Show only groups with a file or folder name containing this text"] = "Nur Gruppen zeigen, deren Datei- oder Ordnername diesen Text enthält",
        ["Sort groups"] = "Gruppen sortieren",
        ["Most space to free"] = "Meiste Ersparnis",
        ["Largest files"] = "Größte Dateien",
        ["Newest"] = "Neueste",
        ["Clean up all…"] = "Alle aufräumen …",
        ["Keep the best copy of every group and move the others to the quarantine folder. Shows the full list first."] =
            "Behält in jeder Gruppe die beste Kopie und verschiebt die anderen in den Quarantäne-Ordner. Zeigt vorher die vollständige Liste.",
        ["to free"] = "frei",
        ["No duplicates to show"] = "Keine Duplikate anzuzeigen",
        ["Add folders in the Folders tab, then click Scan library and Find duplicates. If you searched, clear the search box."] =
            "Füge im Tab „Ordner“ Ordner hinzu und klicke dann auf „Bibliothek scannen“ und „Duplikate finden“. Falls du gesucht hast, leere das Suchfeld.",
        ["{0} files · #{1}"] = "{0} Dateien · #{1}",
        ["saves {0}"] = "spart {0}",
        ["No duplicates found yet."] = "Noch keine Duplikate gefunden.",
        ["{0} groups · {1} extra copies · {2} to free"] = "{0} Gruppen · {1} überzählige Kopien · {2} frei",

        // Folders
        ["Add folder…"] = "Ordner hinzufügen …",
        ["or paste a path"] = "oder Pfad einfügen",
        ["Add"] = "Hinzufügen",
        ["Checked folders are scanned. Changes are saved automatically."] = "Angehakte Ordner werden gescannt. Änderungen werden automatisch gespeichert.",
        ["Include this folder when scanning"] = "Diesen Ordner beim Scannen einbeziehen",
        ["No folders yet"] = "Noch keine Ordner",
        ["Add the folders that contain your videos."] = "Füge die Ordner hinzu, in denen deine Videos liegen.",
        ["Include subfolders"] = "Unterordner einbeziehen",
        ["Skip paths containing… (e.g. temp;cache;whatsapp)"] = "Pfade überspringen, die … enthalten (z. B. temp;cache;whatsapp)",
        ["Remove folder"] = "Ordner entfernen",
        ["Stops scanning this folder. No files are deleted."] = "Dieser Ordner wird nicht mehr gescannt. Es werden keine Dateien gelöscht.",

        // Settings
        ["Appearance"] = "Darstellung",
        ["Language"] = "Sprache",
        ["Mode"] = "Modus",
        ["Color scheme"] = "Farbschema",
        ["System"] = "System",
        ["Light"] = "Hell",
        ["Dark"] = "Dunkel",
        ["Violet"] = "Violett",
        ["Miami Sunset"] = "Miami Sunset",
        ["Ocean"] = "Ozean",
        ["Emerald"] = "Smaragd",
        ["Ruby"] = "Rubin",
        ["Files"] = "Dateien",
        ["Database"] = "Datenbank",
        ["Change…"] = "Ändern …",
        ["Quarantine folder"] = "Quarantäne-Ordner",
        ["Browse…"] = "Durchsuchen …",
        ["Quarantined files can be restored in the Quarantine tab."] =
            "Dateien in Quarantäne kannst du im Tab „Quarantäne“ wiederherstellen.",
        ["Clean-up moves the copies"] = "Aufräumen verschiebt die Kopien",
        ["to the quarantine folder"] = "in den Quarantäne-Ordner",
        ["to the Recycle Bin"] = "in den Papierkorb",
        ["Detection"] = "Erkennung",
        ["Find mirrored copies"] = "Gespiegelte Kopien finden",
        ["Also match videos flipped left to right. No extra time."] = "Findet auch seitenverkehrte Videos. Kostet keine zusätzliche Zeit.",
        ["Confirm close calls by the soundtrack"] = "Knappe Fälle per Tonspur bestätigen",
        ["Compares two minutes of audio when the picture comparison is borderline."] = "Vergleicht zwei Minuten Ton, wenn der Bildvergleich knapp ausfällt.",
        ["Find shortened copies"] = "Gekürzte Kopien finden",
        ["Copies with a cut intro, credits or ads. The first run takes up to ~65 % longer; later runs reuse the results."] =
            "Kopien mit gekürztem Intro, Abspann oder Werbung. Der erste Lauf dauert bis zu ~65 % länger, spätere nutzen die Ergebnisse weiter.",
        ["Replace previous results"] = "Bisherige Ergebnisse ersetzen",
        ["Off keeps the groups of earlier runs as well."] = "Aus: Gruppen früherer Läufe bleiben zusätzlich erhalten.",
        ["Length tolerance"] = "Längentoleranz",
        ["Videos whose lengths differ by more than this many seconds are never compared."] =
            "Videos, deren Länge sich um mehr als so viele Sekunden unterscheidet, werden nie verglichen.",
        ["Strictness"] = "Strenge",
        ["Maximum picture difference (0–64). Lower finds fewer, surer duplicates; default 10."] =
            "Maximaler Bildunterschied (0–64). Niedriger findet weniger, aber sicherere Duplikate; Standard 10.",
        ["Min. group size"] = "Min. Gruppengröße",
        ["Compared frames"] = "Verglichene Bilder",
        ["Positions in percent of the length, comma separated."] = "Positionen in Prozent der Länge, durch Komma getrennt.",
        ["Reset to defaults"] = "Auf Standard zurücksetzen",

        // Group details and file cards
        ["Nothing selected"] = "Nichts ausgewählt",
        ["Select a group on the left to compare its copies side by side."] = "Wähle links eine Gruppe, um ihre Kopien nebeneinander zu vergleichen.",
        ["Not duplicates"] = "Keine Duplikate",
        ["These files are different videos. Removes the group; later runs will not group them again."] =
            "Diese Dateien sind verschiedene Videos. Entfernt die Gruppe; spätere Läufe fassen sie nicht wieder zusammen.",
        ["Keep best, quarantine others"] = "Beste behalten, Rest in Quarantäne",
        ["Moves every file except the best one (or the files marked as keep) to the quarantine folder. Asks first."] =
            "Verschiebt alle Dateien außer der besten (oder den als „Behalten“ markierten) in den Quarantäne-Ordner. Fragt vorher nach.",
        ["Open"] = "Öffnen",
        ["Show in Explorer"] = "Im Explorer zeigen",
        ["Copy path"] = "Pfad kopieren",
        ["Keep this file"] = "Diese Datei behalten",
        ["Move to quarantine"] = "In Quarantäne verschieben",
        ["Not a duplicate"] = "Kein Duplikat",
        ["Duplicate groups"] = "Duplikatgruppen",
        ["Library folders"] = "Bibliotheksordner",
        ["Delete"] = "Löschen",
        ["Move this file to the Recycle Bin."] = "Diese Datei in den Papierkorb verschieben.",
        ["BEST"] = "BESTE",
        ["KEEP"] = "BEHALTEN",
        ["MISSING"] = "FEHLT",
        ["Keep"] = "Behalten",
        ["Always keep this file; clean-up will never move it."] = "Diese Datei immer behalten; Aufräumen verschiebt sie nie.",
        ["More actions"] = "Weitere Aktionen",
        ["Preview frame"] = "Vorschaubild",
        ["Best copy: {0}. "] = "Beste Kopie: {0}. ",
        ["Reference: the other files were compared with this one"] = "Referenz: Die anderen Dateien wurden mit dieser verglichen",
        ["Picture {0:P0} similar"] = "Bild zu {0:P0} ähnlich",
        ["mirrored"] = "gespiegelt",
        ["{0} shorter, cut at the end"] = "{0} kürzer, am Ende gekürzt",
        ["{0} shorter, cut at the start"] = "{0} kürzer, am Anfang gekürzt",
        ["{0} longer, extra footage at the end"] = "{0} länger, zusätzliches Material am Ende",
        ["{0} longer, extra footage at the start"] = "{0} länger, zusätzliches Material am Anfang",
        ["confirmed by the soundtrack ({0:P0} match)"] = "durch die Tonspur bestätigt ({0:P0} Übereinstimmung)",
        ["confirmed by comparing scenes"] = "durch Szenenvergleich bestätigt",
        ["marked as keep"] = "als „Behalten“ markiert",
        ["only file"] = "einzige Datei",
        ["highest resolution ({0}×{1})"] = "höchste Auflösung ({0}×{1})",
        ["same resolution, largest file (most detail)"] = "gleiche Auflösung, größte Datei (meiste Details)",
        ["same resolution and size, newest"] = "gleiche Auflösung und Größe, neueste",
        ["same quality as the others"] = "gleiche Qualität wie die anderen",

        // Dialogs and pickers
        ["Delete file"] = "Datei löschen",
        ["Permanently delete this file? This cannot be undone.\n\n{0}"] = "Diese Datei endgültig löschen? Das kann nicht rückgängig gemacht werden.\n\n{0}",
        ["Delete permanently"] = "Endgültig löschen",
        ["Clean up duplicates"] = "Duplikate aufräumen",
        ["all groups"] = "alle Gruppen",
        ["group #{0}"] = "Gruppe #{0}",
        ["Clean up {0}: move {1} files from {2} groups to the quarantine folder and free {3}."] =
            "{0} aufräumen: {1} Dateien aus {2} Gruppen in den Quarantäne-Ordner verschieben und {3} freigeben.",
        ["Each group keeps its best copy, or the files you marked as keep. Nothing is deleted: the files can be moved back from\n{0}"] =
            "Jede Gruppe behält ihre beste Kopie bzw. die als „Behalten“ markierten Dateien. Es wird nichts gelöscht: Die Dateien können zurückgeholt werden aus\n{0}",
        ["Move {0} files"] = "{0} Dateien verschieben",
        ["keep"] = "behalten",
        ["move"] = "verschieben",
        ["Select folder to scan"] = "Ordner zum Scannen wählen",
        ["Select SQLite Database"] = "SQLite-Datenbank wählen",
        ["SQLite Database"] = "SQLite-Datenbank",
        ["Select quarantine folder"] = "Quarantäne-Ordner wählen",

        // Status messages
        ["Ready."] = "Bereit.",
        ["Add a folder with videos to get started."] = "Füge einen Ordner mit Videos hinzu, um loszulegen.",
        ["Scan the library, then find duplicates."] = "Scanne die Bibliothek und suche dann nach Duplikaten.",
        ["{0} not found. Install FFmpeg and add its bin folder to PATH, then restart VideoDedupe; scanning and detection need it."] =
            "{0} nicht gefunden. Installiere FFmpeg, füge dessen bin-Ordner zum PATH hinzu und starte VideoDedupe neu; Scannen und Erkennung brauchen es.",
        [" and "] = " und ",
        ["Error: {0}"] = "Fehler: {0}",
        ["Loading folders..."] = "Ordner werden geladen …",
        ["{0} folders."] = "{0} Ordner.",
        ["Please enter a folder path."] = "Bitte einen Ordnerpfad eingeben.",
        ["Folder does not exist: {0}"] = "Ordner existiert nicht: {0}",
        ["Adding the folder failed: {0}"] = "Ordner konnte nicht hinzugefügt werden: {0}",
        ["Added folder: {0}"] = "Ordner hinzugefügt: {0}",
        ["Folder removed from the library (files stay where they are)."] = "Ordner aus der Bibliothek entfernt (die Dateien bleiben, wo sie sind).",
        ["Folder settings not saved: {0}"] = "Ordner-Einstellungen nicht gespeichert: {0}",
        ["Scanning..."] = "Scan läuft …",
        ["Scan finished: {0} videos, {1} could not be read."] = "Scan fertig: {0} Videos, {1} nicht lesbar.",
        ["Scan canceled."] = "Scan abgebrochen.",
        ["Scan failed: {0}"] = "Scan fehlgeschlagen: {0}",
        ["Finding duplicates..."] = "Duplikatsuche läuft …",
        ["Search finished: {0} groups."] = "Suche fertig: {0} Gruppen.",
        ["Search canceled."] = "Suche abgebrochen.",
        ["Search failed: {0}"] = "Suche fehlgeschlagen: {0}",
        ["Loading groups..."] = "Gruppen werden geladen …",
        ["Loaded {0} groups."] = "{0} Gruppen geladen.",
        ["Thumbnails failed: {0}"] = "Vorschaubilder fehlgeschlagen: {0}",
        ["File not found."] = "Datei nicht gefunden.",
        ["Opened file."] = "Datei geöffnet.",
        ["Open failed: {0}"] = "Öffnen fehlgeschlagen: {0}",
        ["Opened file location."] = "Speicherort geöffnet.",
        ["Explorer failed: {0}"] = "Explorer fehlgeschlagen: {0}",
        ["Path copied to clipboard."] = "Pfad in die Zwischenablage kopiert.",
        ["Clipboard not available."] = "Zwischenablage nicht verfügbar.",
        ["Copy failed: {0}"] = "Kopieren fehlgeschlagen: {0}",
        ["Delete canceled."] = "Löschen abgebrochen.",
        ["Deleted: {0}"] = "Gelöscht: {0}",
        ["Delete failed: {0}"] = "Löschen fehlgeschlagen: {0}",
        ["Marked as keep: {0}"] = "Als „Behalten“ markiert: {0}",
        ["File missing."] = "Datei fehlt.",
        ["Moved to quarantine: {0}"] = "In Quarantäne verschoben: {0}",
        ["Marked as not a duplicate: {0}"] = "Als „Kein Duplikat“ markiert: {0}",
        ["Group #{0} marked as not duplicates."] = "Gruppe #{0} als „Keine Duplikate“ markiert.",
        ["Nothing to clean up."] = "Nichts aufzuräumen.",
        ["Clean-up canceled."] = "Aufräumen abgebrochen.",
        ["Moving {0}/{1}: {2}"] = "Verschiebe {0}/{1}: {2}",
        ["Moved {0} of {1} files to quarantine."] = "{0} von {1} Dateien in Quarantäne verschoben.",
        ["Moved {0} of {1} files to quarantine (canceled)."] = "{0} von {1} Dateien in Quarantäne verschoben (abgebrochen).",
        [" {0} failed: {1}"] = " {0} fehlgeschlagen: {1}",

        // Undo, quarantine tab, folder priorities, report
        ["Export as CSV…"] = "Als CSV exportieren …",
        ["Keep best, delete others"] = "Beste behalten, Rest löschen",
        ["Keep the best copy of every group and move the others to the Recycle Bin. Shows the full list first."] = "In jeder Gruppe die beste Kopie behalten, die anderen in den Papierkorb verschieben. Zeigt vorher die vollständige Liste.",
        ["Moves every file except the best one (or the files marked as keep) to the Recycle Bin. Asks first."] = "Verschiebt alle Dateien außer der besten (oder den als „Behalten“ markierten) in den Papierkorb. Fragt vorher.",
        ["Press this number to select the file"] = "Diese Zahl drücken, um die Datei auszuwählen",
        ["in a preferred folder"] = "liegt in einem bevorzugten Ordner",
        ["the other copies are in folders to clear first"] = "die anderen Kopien liegen in Ordnern, die zuerst geleert werden",
        ["When a video is in several folders"] = "Wenn ein Video in mehreren Ordnern liegt",
        ["Keep files from here"] = "Dateien von hier behalten",
        ["Normal"] = "Normal",
        ["Remove copies from here first"] = "Kopien von hier zuerst entfernen",
        ["Decides which copy is kept before resolution and size do. Example: keep your archive, clear Downloads first."] = "Entscheidet vor Auflösung und Größe, welche Kopie bleibt. Beispiel: Archiv behalten, Downloads zuerst leeren.",
        ["Quarantine ({0})"] = "Quarantäne ({0})",
        ["{0} files · {1}"] = "{0} Dateien · {1}",
        ["Open folder"] = "Ordner öffnen",
        ["Delete all…"] = "Alle löschen …",
        ["Move every file in the quarantine folder to the Recycle Bin. Asks first."] = "Alle Dateien im Quarantäne-Ordner in den Papierkorb verschieben. Fragt vorher.",
        ["Quarantined files"] = "Dateien in Quarantäne",
        ["Quarantine is empty"] = "Quarantäne ist leer",
        ["Files you move to quarantine show up here. You can put them back or delete them for good."] = "Hier erscheinen Dateien, die du in Quarantäne verschiebst. Du kannst sie zurücklegen oder endgültig löschen.",
        ["Restore"] = "Wiederherstellen",
        ["Move the file back to its original folder."] = "Die Datei in ihren ursprünglichen Ordner zurückverschieben.",
        ["Clean up {0}: move {1} files from {2} groups to the Recycle Bin and free {3}."] = "{0} aufräumen: {1} Dateien aus {2} Gruppen in den Papierkorb verschieben und {3} freigeben.",
        ["Each group keeps its best copy, or the files you marked as keep. The files can be restored from the Recycle Bin."] = "Jede Gruppe behält ihre beste Kopie oder die als „Behalten“ markierten Dateien. Aus dem Papierkorb lassen sich die Dateien wiederherstellen.",
        ["Delete {0} files"] = "{0} Dateien löschen",
        ["delete"] = "löschen",
        ["Empty quarantine"] = "Quarantäne leeren",
        ["Move all {0} files ({1}) from the quarantine folder to the Recycle Bin?"] = "Alle {0} Dateien ({1}) aus dem Quarantäne-Ordner in den Papierkorb verschieben?",
        ["Permanently delete all {0} files ({1}) in the quarantine folder? This cannot be undone."] = "Alle {0} Dateien ({1}) im Quarantäne-Ordner endgültig löschen? Das kann nicht rückgängig gemacht werden.",
        ["Export duplicates"] = "Duplikate exportieren",
        ["CSV table"] = "CSV-Tabelle",
        ["Group"] = "Gruppe",
        ["Action"] = "Aktion",
        ["File name"] = "Dateiname",
        ["Folder"] = "Ordner",
        ["Size (MB)"] = "Größe (MB)",
        ["Resolution"] = "Auflösung",
        ["Duration (s)"] = "Dauer (s)",
        ["Bitrate (Mbit/s)"] = "Bitrate (Mbit/s)",
        ["Codec"] = "Codec",
        ["Picture similarity (%)"] = "Bildähnlichkeit (%)",
        ["Modified"] = "Geändert",
        ["remove"] = "entfernen",
        ["Undone: {0}"] = "Rückgängig gemacht: {0}",
        ["Deleting {0}/{1}: {2}"] = "Lösche {0}/{1}: {2}",
        ["Moved {0} of {1} files to the Recycle Bin."] = "{0} von {1} Dateien in den Papierkorb verschoben.",
        ["Moved {0} of {1} files to the Recycle Bin (canceled)."] = "{0} von {1} Dateien in den Papierkorb verschoben (abgebrochen).",
        ["Exported {0} groups to {1}"] = "{0} Gruppen exportiert nach {1}",
        ["Export failed: {0}"] = "Export fehlgeschlagen: {0}",
        ["Restored: {0}"] = "Wiederhergestellt: {0}",
        ["Restore failed: {0}"] = "Wiederherstellen fehlgeschlagen: {0}",
        ["Deleted {0} files from the quarantine folder."] = "{0} Dateien aus dem Quarantäne-Ordner gelöscht.",
        ["Deleted {0} files from the quarantine folder, {1} failed."] = "{0} Dateien aus dem Quarantäne-Ordner gelöscht, {1} fehlgeschlagen.",

        // Update check, best-copy rules, identical files, excerpts, compare, history
        ["Keys: ↑↓ group · ←→ or 1–9 file · Del delete · Q quarantine · B keep · V compare · N not a duplicate · Enter open · Ctrl+Z undo"] = "Tasten: ↑↓ Gruppe · ←→ oder 1–9 Datei · Entf löschen · Q Quarantäne · B behalten · V vergleichen · N kein Duplikat · Enter öffnen · Strg+Z rückgängig",
        ["Version {0} is available"] = "Version {0} ist verfügbar",
        ["Version {0}"] = "Version {0}",
        ["Opens the download page of the new version."] = "Öffnet die Download-Seite der neuen Version.",
        ["Check for updates at start"] = "Beim Start nach Updates suchen",
        ["Best copy"] = "Beste Kopie",
        ["Files marked as keep and folder priorities come first, then the highest resolution. Between copies of the same resolution:"] = "Zuerst zählen „Behalten“ und die Ordner-Priorität, dann die höchste Auflösung. Bei gleicher Auflösung:",
        ["Codec"] = "Codec",
        ["File format"] = "Dateiformat",
        ["Then keep the"] = "Danach behalten",
        ["No preference"] = "Egal",
        ["HEVC, AV1 or VP9 (smaller files)"] = "HEVC, AV1 oder VP9 (kleinere Dateien)",
        ["H.264 (plays everywhere)"] = "H.264 (läuft überall)",
        ["Larger file (more detail)"] = "Größere Datei (mehr Details)",
        ["Smaller file (less space)"] = "Kleinere Datei (weniger Platz)",
        ["same resolution, preferred codec ({0})"] = "gleiche Auflösung, bevorzugter Codec ({0})",
        ["same resolution, preferred file format"] = "gleiche Auflösung, bevorzugtes Dateiformat",
        ["same resolution, smallest file"] = "gleiche Auflösung, kleinste Datei",
        ["Identical file: same size and content"] = "Identische Datei: gleiche Größe und gleicher Inhalt",
        ["excerpt of the longer video, from {0}"] = "Ausschnitt aus dem längeren Video, ab {0}",
        ["contains the excerpt from {0}"] = "enthält den Ausschnitt ab {0}",
        ["Find excerpts"] = "Ausschnitte finden",
        ["Clips of 20 s to 10 min cut out of a longer video. Slow the first time: every video is read completely once (about 3–10 s per 1 GB file); later runs only read new files."] = "Clips von 20 s bis 10 min, die aus einem längeren Video geschnitten wurden. Beim ersten Mal langsam: Jedes Video wird einmal ganz gelesen (etwa 3–10 s pro 1-GB-Datei); spätere Läufe lesen nur neue Dateien.",
        ["Finding identical files"] = "Identische Dateien werden gesucht",
        ["Reading keyframes for excerpts"] = "Schlüsselbilder für Ausschnitte werden gelesen",
        ["Sampling short videos for excerpts"] = "Kurze Videos werden für Ausschnitte abgetastet",
        ["Searching excerpts"] = "Ausschnitte werden gesucht",
        ["Checking excerpts"] = "Ausschnitte werden geprüft",
        ["Compare"] = "Vergleichen",
        ["Compare copies"] = "Kopien vergleichen",
        ["Compare with the best copy"] = "Mit der besten Kopie vergleichen",
        ["Show the selected file and the best copy side by side, at full resolution (V)."] = "Die ausgewählte Datei und die beste Kopie nebeneinander in voller Auflösung zeigen (V).",
        ["Fit"] = "Einpassen",
        ["Zoom"] = "Zoom",
        ["Position"] = "Position",
        ["Back 2 % (Left arrow)"] = "2 % zurück (Pfeil links)",
        ["Forward 2 % (Right arrow)"] = "2 % vor (Pfeil rechts)",
        ["Could not read the frame: {0}"] = "Bild konnte nicht gelesen werden: {0}",
        ["History"] = "Verlauf",
        ["History…"] = "Verlauf …",
        ["Close"] = "Schließen",
        ["Nothing deleted or moved yet."] = "Noch nichts gelöscht oder verschoben.",
        ["Deleted: {0} files, {1}"] = "Gelöscht: {0} Dateien, {1}",
        ["Quarantined: {0} files, {1}"] = "In Quarantäne: {0} Dateien, {1}",
        ["Restored: {0} files, {1}"] = "Wiederhergestellt: {0} Dateien, {1}",
        ["deleted"] = "gelöscht",
        ["quarantined"] = "in Quarantäne verschoben",
        ["restored"] = "wiederhergestellt",
        ["deleted from quarantine"] = "aus der Quarantäne gelöscht",
    };
}
