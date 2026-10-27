# Trello Focus Display

A minimal, distraction-free full-screen display of your top Trello cards. Static HTML, no server, no build step.

## Setup

1. **Get an API key and token**: go to [trello.com/power-ups/admin](https://trello.com/power-ups/admin), create/select a Power-Up to get an API key, then generate a token authorized for read/write access to your boards.

2. **Find your list ID**: open any card in the list you want to display, add `.json` to the card's URL (e.g. `https://trello.com/c/abc123.json`), open it in your browser, and read the `idList` field.

   If your `completeMode` is `"move"`, also find the ID of the list you want completed cards moved to, the same way (`idList` of any card already in that list, or check the board's lists via the API).

3. **Copy the config**:

   ```
   copy config.example.js config.js
   ```

   Edit `config.js` and fill in `apiKey`, `token`, `listId`, and (if using `completeMode: "move"`) `doneListId`. `config.js` is gitignored since it contains your token.

4. **Open `index.html`** directly in a browser (no server needed). Put the browser in full-screen/kiosk mode for a clean display.

## Controls

- **Right arrow**: next card
- **Left arrow**: previous card
- **C**: mark the current card complete (behavior depends on `completeMode`)

## Config options

| Field | Description |
|---|---|
| `apiKey` / `token` | Trello API credentials |
| `listId` | List to pull cards from |
| `topN` | Number of cards to show, in board order |
| `refreshSeconds` | How often to re-fetch the list (`0` disables) |
| `completeMode` | `"move"` (move to `doneListId`), `"dueComplete"` (check the card's completion circle, leave it in place), `"dueCompleteBottom"` (check it and move it to the bottom of the list), or `"archive"` (archive the card) |
| `doneListId` | Target list for `completeMode: "move"` |
