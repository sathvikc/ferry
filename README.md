# Ferry

Share files and folders with every device on your local network, from the browser, with no install on the other devices and no dependencies on the server.

## Run

```
npm start
```

It prints the addresses to open on other devices, for example `http://192.168.1.102:8080`. Needs Node 20.15 or newer.

- `PORT=9000 npm start` changes the port.
- `UPLOAD_DIR=/path npm start` changes where files are stored (default `./uploads`).

## What it does

- Upload files or whole folders by drag and drop, paste, or the Files and Folder buttons; each upload shows its own progress bar.
- Every open page updates live over Server-Sent Events: new files, deletes, renames and who is online.
- Each browser gets a random name (editable) and files are grouped by who shared them.
- Folders download as one zip, streamed from the stored files with an exact size, so the browser shows real progress; files over 4 GB are supported (ZIP64).
- One click deletes a file or folder for everyone.

## Files

- `server.js`: HTTP routes, upload streaming, metadata in `uploads/meta.json`, live events.
- `zip.js`: streaming ZIP writer (stored entries, ZIP64 when needed).
- `public/index.html`: the whole UI, vanilla JS and CSS.

## Security

There is none by design: anyone on the same network can download or delete anything. Run it on networks you trust, not public Wi-Fi.
