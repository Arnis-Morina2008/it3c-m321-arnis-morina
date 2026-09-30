# M321 Chat-App — Projektregeln (IT3c)

Unterrichtsprojekt. Der Code wird von Schülerinnen und Schülern gelesen, nicht nur
ausgeführt. Verständlichkeit schlägt Eleganz — immer.

## Sprache

- **Code auf Englisch**: Klassen, Methoden, Variablen, Dateinamen, Log-Meldungen.
- **Alles andere auf Deutsch**: Kommentare, Javadoc, Commit-Messages, Doku, README.

## Code-Stil

- **Einfach und lesbar.** Der langweilige Weg ist der richtige Weg.
- **Keine verschachtelten Aufrufe.** Ein Ergebnis pro Zeile, in eine benannte
  Variable. Nicht `save(map(load(id)))`, sondern:
  ```java
  User user = load(id);
  UserDto dto = map(user);
  save(dto);
  ```
  Das gilt auch für Streams: lieber eine `for`-Schleife, die man laut vorlesen kann.
- **Viele Kommentare.** Jede Klasse und jede Methode bekommt einen Kommentar, der
  erklärt *warum* sie existiert, nicht *was* die nächste Zeile tut.
- **Sprechende Namen.** `messageRepository`, nicht `repo`. Keine Abkürzungen ausser
  `i`, `j` in Schleifen.
- **Kurze Methoden.** Eine Methode macht eine Sache. Passt sie nicht auf den
  Beamer, ist sie zu lang.
- **Keine Magie.** Keine Reflection, keine cleveren Tricks, keine Framework-Features,
  die man nicht in zwei Sätzen erklären kann.
- **Lombok für Boilerplate.** `@Slf4j` für den Logger, `@RequiredArgsConstructor` für
  die Konstruktor-Injektion. Beides spart eine Zeile, die in jeder Klasse gleich
  aussieht. Für reine Datenklassen nehmen wir Java-`record` — das kann Java selbst.
- **Keine Vorrats-Abstraktionen.** Kein Interface mit einer einzigen Implementierung,
  keine Factory für ein einziges Produkt. Wenn wir es später brauchen, bauen wir es
  später.

## Architektur-Rahmen (vorgegeben)

- **Java 21** im Backend.
- **Keycloak** als Login-Dienst (IDP). Kein selbstgebautes Login.
- **Message Queue** für den Nachrichtentransport.
- **docker-compose** bildet die gesamte Microservice-Architektur ab.
- Alle Dienste sprechen über ein **internes Docker-Netzwerk** miteinander.
- **Nur die Web-App ist über localhost erreichbar.** Alle anderen Container
  veröffentlichen keine Ports nach aussen.

## Arbeitsweise

- **Erst verstehen, dann ändern.** Vorhandenen Code lesen, bevor er angefasst wird.
- **Kleine Schritte.** Ein Thema pro Commit, damit die Klasse mitlesen kann.
- **Nichts behaupten, was nicht geprüft wurde.** "Läuft" erst sagen, wenn es
  wirklich gestartet wurde.
- **Keine Geheimnisse im Repository.** Passwörter und Secrets nur in `.env`
  (steht in `.gitignore`), im Repo nur Beispielwerte für die Schulung.
