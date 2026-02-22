import os
import shutil
import sqlite3

from PySide6.QtCore import QEvent, QPoint, QTimer, Qt, QUrl, Signal
from PySide6.QtGui import QColor, QDesktopServices, QIcon, QImage, QPainter, QPixmap, QPolygon
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QComboBox,
    QDialog,
    QFileDialog,
    QFrame,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QToolButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
    QHeaderView,
)

import app_core as core


class CyclicComboBox(QComboBox):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._bind_line_edit()

    def setEditable(self, editable):
        super().setEditable(editable)
        self._bind_line_edit()

    def _bind_line_edit(self):
        le = self.lineEdit()
        if le is not None:
            le.installEventFilter(self)

    def _cycle_key(self, key):
        if self.count() <= 0:
            return False
        idx = self.currentIndex()
        if idx < 0:
            self.setCurrentIndex(0)
            return True
        if key == Qt.Key_Down:
            self.setCurrentIndex((idx + 1) % self.count())
            return True
        if key == Qt.Key_Up:
            self.setCurrentIndex((idx - 1) % self.count())
            return True
        return False

    def keyPressEvent(self, event):
        if self._cycle_key(event.key()):
            event.accept()
            return
        super().keyPressEvent(event)

    def eventFilter(self, obj, event):
        if obj is self.lineEdit() and event.type() == QEvent.KeyPress:
            if self._cycle_key(event.key()):
                event.accept()
                return True
        return super().eventFilter(obj, event)


class TrapezoidToggleButton(QPushButton):
    def __init__(self, text="", parent=None):
        super().__init__(text, parent)
        self.setFixedSize(24, 58)
        self.setCursor(Qt.PointingHandCursor)
        self.setFlat(True)
        self._hover = False

    def enterEvent(self, event):
        self._hover = True
        self.update()
        super().enterEvent(event)

    def leaveEvent(self, event):
        self._hover = False
        self.update()
        super().leaveEvent(event)

    def paintEvent(self, _event):
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing, True)
        w = self.width()
        h = self.height()
        poly = QPolygon([QPoint(4, 0), QPoint(w - 3, 6), QPoint(w - 3, h - 6), QPoint(4, h)])
        bg = QColor("#d8dee7" if not self._hover else "#c7d3e4")
        p.setPen(QColor("#b7c6d8"))
        p.setBrush(bg)
        p.drawPolygon(poly)
        p.setPen(QColor("#22415f"))
        p.drawText(self.rect(), Qt.AlignCenter, self.text())
        p.end()


class AttachmentChipButton(QPushButton):
    singleClicked = Signal()
    doubleClicked = Signal()

    def __init__(self, text="", parent=None):
        super().__init__(text, parent)
        self._click_timer = QTimer(self)
        self._click_timer.setSingleShot(True)
        self._click_timer.timeout.connect(self.singleClicked.emit)

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            if self._click_timer.isActive():
                self._click_timer.stop()
                self.doubleClicked.emit()
            else:
                self._click_timer.start(max(120, int(QApplication.doubleClickInterval())))
            event.accept()
            return
        super().mousePressEvent(event)


def _mk_check(checked=False):
    it = QTableWidgetItem("")
    it.setFlags(Qt.ItemIsEnabled | Qt.ItemIsUserCheckable)
    it.setTextAlignment(Qt.AlignCenter)
    it.setCheckState(Qt.Checked if checked else Qt.Unchecked)
    return it


def _next_template_no():
    conn = sqlite3.connect(core.DB_FILE)
    try:
        row = conn.execute("SELECT COALESCE(MAX(template_no), 0) FROM checklist_templates").fetchone()
        return int((row[0] if row else 0) or 0) + 1
    finally:
        conn.close()


def _template_organization_options():
    vals = list(core.get_user_setting("template_manual_org_options", []) or [])
    conn = sqlite3.connect(core.DB_FILE)
    try:
        try:
            rows = conn.execute(
                "SELECT DISTINCT TRIM(COALESCE(organization,'')) "
                "FROM checklist_templates WHERE TRIM(COALESCE(organization,''))!='' "
                "ORDER BY organization"
            ).fetchall()
            vals.extend([str(r[0]).strip() for r in rows if r and str(r[0]).strip()])
        except Exception:
            pass
    finally:
        conn.close()
    out = []
    seen = set()
    for v in vals:
        k = v.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(v)
    if "general" not in seen:
        out.insert(0, "General")
    return out


def _save_manual_org_option(org_name):
    org = str(org_name or "").strip()
    if not org:
        return
    raw = core.get_user_setting("template_manual_org_options", []) or []
    vals = [str(x).strip() for x in raw if str(x).strip()]
    if org.lower() not in {v.lower() for v in vals}:
        vals.append(org)
        core.set_user_setting("template_manual_org_options", vals)


def _ask_template_meta(parent, default_name="", default_org="", default_desc=""):
    d = QDialog(parent)
    d.setWindowTitle("Template Details")
    d.resize(560, 220)
    root = QVBoxLayout(d)
    form = QFormLayout()
    no_edit = QLineEdit(str(_next_template_no()))
    org_combo = CyclicComboBox()
    org_combo.setEditable(True)
    org_combo.addItems(_template_organization_options())
    if str(default_org or "").strip():
        org_combo.setCurrentText(str(default_org).strip())
    elif org_combo.count() > 0:
        org_combo.setCurrentIndex(0)
    name_edit = QLineEdit(default_name or "Template")
    desc_edit = QLineEdit(default_desc)
    form.addRow("Template Number", no_edit)
    form.addRow("Organization", org_combo)
    form.addRow("Template Name", name_edit)
    form.addRow("Description", desc_edit)
    root.addLayout(form)
    row = QHBoxLayout()
    ok = QPushButton("Save")
    ok.setObjectName("PrimaryButton")
    cancel = QPushButton("Cancel")
    row.addStretch(1)
    row.addWidget(cancel)
    row.addWidget(ok)
    root.addLayout(row)
    ok.clicked.connect(d.accept)
    cancel.clicked.connect(d.reject)
    if d.exec() != QDialog.Accepted:
        return None
    no_txt = str(no_edit.text() or "").strip()
    no_val = int(no_txt) if no_txt.isdigit() else _next_template_no()
    org = str(org_combo.currentText() or "").strip() or "General"
    _save_manual_org_option(org)
    name = str(name_edit.text() or "").strip()
    desc = str(desc_edit.text() or "").strip()
    if not name:
        QMessageBox.warning(parent, "Template", "Template name is required.")
        return None
    return {"template_no": no_val, "organization": org, "template_name": name, "description": desc}


class TemplateImportDialog(QDialog):
    def __init__(self, parent, project_id, project_root, template_ids, target_subfolder="Working Docs"):
        super().__init__(parent)
        self.project_id = int(project_id)
        self.project_root = str(project_root or "")
        self.template_ids = [int(x) for x in template_ids]
        self.target_subfolder = str(target_subfolder or "Working Docs").strip() or "Working Docs"
        self.views = []
        self.setWindowTitle("Import Templates")
        self.resize(1000, 700)
        root = QVBoxLayout(self)
        self.tabs = QTabWidget()
        root.addWidget(self.tabs, 1)
        b = QHBoxLayout()
        self.ok_btn = QPushButton("Import Selected")
        self.ok_btn.setObjectName("PrimaryButton")
        close_btn = QPushButton("Close")
        b.addStretch(1)
        b.addWidget(self.ok_btn)
        b.addWidget(close_btn)
        root.addLayout(b)
        close_btn.clicked.connect(self.reject)
        self.ok_btn.clicked.connect(self._import_selected)
        self._load_tabs()

    def _load_tabs(self):
        conn = sqlite3.connect(core.DB_FILE)
        try:
            for tid in self.template_ids:
                meta = conn.execute(
                    "SELECT id, COALESCE(template_no,0), COALESCE(organization,''), COALESCE(template_name,'') FROM checklist_templates WHERE id=?",
                    (tid,),
                ).fetchone()
                if not meta:
                    continue
                items = conn.execute(
                    "SELECT id, COALESCE(sr_no,0), COALESCE(req_file_name,''), COALESCE(description,''), COALESCE(subfolder,'Main') FROM checklist_template_items WHERE template_id=? ORDER BY subfolder, sr_no, id",
                    (tid,),
                ).fetchall()
                files = conn.execute(
                    "SELECT f.id, f.template_item_id, COALESCE(i.req_file_name,''), COALESCE(f.file_name,''), COALESCE(f.stored_path,'') FROM checklist_template_item_files f JOIN checklist_template_items i ON i.id=f.template_item_id WHERE i.template_id=? ORDER BY f.id",
                    (tid,),
                ).fetchall()
                self._add_tab(meta, items, files)
        finally:
            conn.close()

    def _add_tab(self, meta, items, files):
        tid, tno, org, name = meta
        page = QWidget()
        root = QVBoxLayout(page)
        row = QHBoxLayout()
        all_f = QPushButton("Select All")
        row.addWidget(all_f)
        row.addStretch(1)
        root.addLayout(row)
        tbl = QTableWidget(0, 7)
        tbl.setHorizontalHeaderLabels(["Import", "Document Name", "Description", "File Name", "ItemID", "FileID", "Path"])
        tbl.setEditTriggers(QAbstractItemView.NoEditTriggers)
        tbl.verticalHeader().setVisible(False)
        tbl.setSelectionBehavior(QAbstractItemView.SelectRows)
        tbl.setSelectionMode(QAbstractItemView.SingleSelection)
        tbl.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        tbl.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        tbl.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        tbl.horizontalHeader().setStretchLastSection(True)
        tbl.setColumnHidden(4, True)
        tbl.setColumnHidden(5, True)
        tbl.setColumnHidden(6, True)
        tbl.setColumnWidth(0, 90)
        tbl.setColumnWidth(1, 260)
        tbl.setColumnWidth(2, 300)
        tbl.setColumnWidth(3, 320)
        root.addWidget(tbl, 1)

        item_rows = []
        for iid, _sr, doc, desc, _folder in items:
            item_rows.append({"item_id": int(iid), "doc": str(doc), "desc": str(desc)})

        files_by_item = {}
        for fid, item_id, _item_name, file_name, path in files:
            iid_int = int(item_id)
            files_by_item.setdefault(iid_int, []).append(
                {"file_id": int(fid), "file_name": str(file_name), "path": str(path)}
            )

        for ir in item_rows:
            related = files_by_item.get(int(ir["item_id"]), [])
            if not related:
                r = tbl.rowCount()
                tbl.insertRow(r)
                tbl.setItem(r, 0, _mk_check(True))
                d = QTableWidgetItem(ir["doc"]); d.setTextAlignment(Qt.AlignCenter); tbl.setItem(r, 1, d)
                de = QTableWidgetItem(ir["desc"]); de.setTextAlignment(Qt.AlignCenter); tbl.setItem(r, 2, de)
                fn = QTableWidgetItem(""); fn.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter); tbl.setItem(r, 3, fn)
                tbl.setItem(r, 4, QTableWidgetItem(str(ir["item_id"])))
                tbl.setItem(r, 5, QTableWidgetItem("0"))
                tbl.setItem(r, 6, QTableWidgetItem(""))
            else:
                for fr in related:
                    r = tbl.rowCount()
                    tbl.insertRow(r)
                    tbl.setItem(r, 0, _mk_check(True))
                    d = QTableWidgetItem(ir["doc"]); d.setTextAlignment(Qt.AlignCenter); tbl.setItem(r, 1, d)
                    de = QTableWidgetItem(ir["desc"]); de.setTextAlignment(Qt.AlignCenter); tbl.setItem(r, 2, de)
                    fn = QTableWidgetItem(fr["file_name"]); fn.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter); tbl.setItem(r, 3, fn)
                    tbl.setItem(r, 4, QTableWidgetItem(str(ir["item_id"])))
                    tbl.setItem(r, 5, QTableWidgetItem(str(fr["file_id"])))
                    tbl.setItem(r, 6, QTableWidgetItem(str(fr["path"])))

        all_f.clicked.connect(lambda: self._set_all(tbl))

        self.views.append({"tid": int(tid), "tbl": tbl})
        self.tabs.addTab(page, f"#{tno} {org}/{name}")

    def _set_all(self, table):
        for r in range(table.rowCount()):
            c = table.item(r, 0)
            if c:
                c.setCheckState(Qt.Checked)

    def _copy_unique(self, src, out_dir, name):
        os.makedirs(out_dir, exist_ok=True)
        base = str(name or os.path.basename(src))
        stem, ext = os.path.splitext(base)
        dst = os.path.join(out_dir, base)
        i = 1
        while os.path.exists(dst):
            dst = os.path.join(out_dir, f"{stem}_{i}{ext}")
            i += 1
        shutil.copy2(src, dst)
        return dst

    def _import_selected(self):
        conn = sqlite3.connect(core.DB_FILE)
        inserted = 0
        copied = 0
        try:
            n = conn.execute("SELECT COUNT(*) FROM checklist_items WHERE project_id=?", (self.project_id,)).fetchone()
            sr = int((n[0] if n else 0) or 0) + 1
            for view in self.views:
                tbl = view["tbl"]
                map_new = {}
                seen_items = set()
                for r in range(tbl.rowCount()):
                    if tbl.item(r, 0).checkState() != Qt.Checked:
                        continue
                    iid_txt = str(tbl.item(r, 4).text() or "")
                    if not iid_txt.isdigit():
                        continue
                    old_id = int(iid_txt)
                    if old_id in seen_items:
                        continue
                    seen_items.add(old_id)
                    doc = str(tbl.item(r, 1).text() or "")
                    desc = str(tbl.item(r, 2).text() or "")
                    sub = self.target_subfolder
                    conn.execute(
                        "INSERT INTO checklist_items (project_id, sr_no, req_file_name, description, subfolder, linked_file_path, status) VALUES (?,?,?,?,?,'','Pending')",
                        (self.project_id, sr, doc, desc, sub),
                    )
                    new_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                    map_new[old_id] = (new_id, sub)
                    sr += 1
                    inserted += 1

                for r in range(tbl.rowCount()):
                    if tbl.item(r, 0).checkState() != Qt.Checked:
                        continue
                    old_item_txt = str(tbl.item(r, 4).text() or "")
                    if not old_item_txt.isdigit():
                        continue
                    old_item = int(old_item_txt)
                    if old_item not in map_new:
                        continue
                    fid_txt = str(tbl.item(r, 5).text() or "")
                    if not fid_txt.isdigit() or int(fid_txt) <= 0:
                        continue
                    src = str(tbl.item(r, 6).text() or "").strip()
                    fname = str(tbl.item(r, 3).text() or "").strip()
                    if not src or not os.path.isfile(src):
                        continue
                    new_id, sub = map_new[old_item]
                    out_dir = self.project_root if sub == "Main" else os.path.join(self.project_root, sub)
                    dst = self._copy_unique(src, out_dir, fname)
                    existing = conn.execute("SELECT COALESCE(linked_file_path,'') FROM checklist_items WHERE id=?", (new_id,)).fetchone()
                    if existing and not str(existing[0] or "").strip():
                        conn.execute("UPDATE checklist_items SET linked_file_path=?, status='Completed' WHERE id=?", (dst, new_id))
                    copied += 1
            conn.commit()
        finally:
            conn.close()
        QMessageBox.information(self, "Import Templates", f"Imported items: {inserted}\nCopied files: {copied}")
        self.accept()


def import_templates_into_project(parent, project_id, project_root):
    std = core.ensure_project_standard_folders(project_root)
    project_root = std.get("project_root", project_root)
    folder_set = {"Main", "Working Docs", "Ready Docs", "Tender Docs"}
    if os.path.isdir(project_root):
        for root_dir, dirs, _ in os.walk(project_root):
            rel = os.path.relpath(root_dir, project_root)
            folder_set.add("Main" if rel in (".", "") else rel)
            for dname in dirs:
                rel_d = os.path.relpath(os.path.join(root_dir, dname), project_root)
                folder_set.add("Main" if rel_d in (".", "") else rel_d)
    folder_options = sorted(folder_set, key=lambda x: (x.lower() not in {"working docs", "ready docs", "tender docs", "main"}, x.lower()))

    conn = sqlite3.connect(core.DB_FILE)
    try:
        rows = conn.execute(
            "SELECT id, COALESCE(template_no,0), COALESCE(organization,''), COALESCE(template_name,''), COALESCE(description,'') FROM checklist_templates ORDER BY template_no, organization, template_name"
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        QMessageBox.information(parent, "Import Templates", "No templates available.")
        return
    d = QDialog(parent)
    d.setWindowTitle("Select Templates")
    d.resize(840, 520)
    root = QVBoxLayout(d)
    top = QHBoxLayout()
    top.addWidget(QLabel("Import To Folder"))
    folder_combo = QComboBox()
    folder_combo.addItems(folder_options)
    idx_work = folder_combo.findText("Working Docs")
    if idx_work >= 0:
        folder_combo.setCurrentIndex(idx_work)
    top.addWidget(folder_combo, 1)
    root.addLayout(top)
    tbl = QTableWidget(0, 5)
    tbl.setHorizontalHeaderLabels(["Select \u2713", "Organization", "No", "Template", "Description"])
    tbl.verticalHeader().setVisible(False)
    tbl.setEditTriggers(QAbstractItemView.NoEditTriggers)
    tbl.setSelectionBehavior(QAbstractItemView.SelectRows)
    tbl.setSelectionMode(QAbstractItemView.SingleSelection)
    tbl.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
    tbl.horizontalHeader().setStretchLastSection(True)
    tbl.setColumnWidth(0, 120)
    tbl.setColumnWidth(1, 170)
    tbl.setColumnWidth(2, 90)
    tbl.setColumnWidth(3, 170)
    root.addWidget(tbl, 1)

    def _set_select_cell(row_idx, checked):
        it = QTableWidgetItem("\u2713" if checked else "")
        it.setTextAlignment(Qt.AlignCenter)
        it.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
        if checked:
            it.setBackground(QColor("#dbeeff"))
            it.setForeground(QColor("#155a8a"))
        else:
            it.setBackground(QColor("#eef4fb"))
            it.setForeground(QColor("#eef4fb"))
        tbl.setItem(row_idx, 0, it)

    selected = set()

    def _toggle_row(row_idx):
        if row_idx < 0 or row_idx >= tbl.rowCount():
            return
        if row_idx in selected:
            selected.remove(row_idx)
            _set_select_cell(row_idx, False)
        else:
            selected.add(row_idx)
            _set_select_cell(row_idx, True)

    for tid, tno, org, name, desc in rows:
        r = tbl.rowCount()
        tbl.insertRow(r)
        _set_select_cell(r, False)
        org_it = QTableWidgetItem(str(org))
        org_it.setData(Qt.UserRole, int(tid))
        tbl.setItem(r, 1, org_it)
        tbl.setItem(r, 2, QTableWidgetItem(str(tno)))
        tbl.setItem(r, 3, QTableWidgetItem(str(name)))
        tbl.setItem(r, 4, QTableWidgetItem(str(desc)))
    tbl.cellClicked.connect(lambda r, c: _toggle_row(r) if c == 0 else None)
    row = QHBoxLayout()
    sel_all = QPushButton("Select All")
    ok = QPushButton("Next")
    ok.setObjectName("PrimaryButton")
    cancel = QPushButton("Cancel")
    row.addWidget(sel_all)
    row.addStretch(1)
    row.addWidget(cancel)
    row.addWidget(ok)
    root.addLayout(row)
    sel_all.clicked.connect(lambda: [selected.add(i) or _set_select_cell(i, True) for i in range(tbl.rowCount())])
    cancel.clicked.connect(d.reject)
    ok.clicked.connect(d.accept)
    if d.exec() != QDialog.Accepted:
        return
    tids = []
    for r in range(tbl.rowCount()):
        if r in selected:
            tid = tbl.item(r, 1).data(Qt.UserRole)
            if isinstance(tid, int):
                tids.append(tid)
    if not tids:
        QMessageBox.information(parent, "Import Templates", "No templates selected.")
        return
    target_folder = str(folder_combo.currentText() or "Working Docs").strip() or "Working Docs"
    dialog = TemplateImportDialog(parent, int(project_id), project_root, tids, target_subfolder=target_folder)
    dialog.exec()


class TemplatesPage(QWidget):
    def __init__(self, controller):
        super().__init__()
        self.controller = controller
        self.current_template_id = None
        self._row_meta = []
        self.current_pdf = None
        self.current_page = 0
        self._rowfit_timer = QTimer(self)
        self._rowfit_timer.setSingleShot(True)
        self._rowfit_timer.timeout.connect(self._fit_rows)
        self._colsave_timer = QTimer(self)
        self._colsave_timer.setSingleShot(True)
        self._colsave_timer.timeout.connect(self._save_column_widths)
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        bar = QHBoxLayout()
        self.new_btn = QPushButton("New Template")
        self.save_btn = QPushButton("Create From Project")
        self.save_btn.setObjectName("PrimaryButton")
        self.open_folder_btn = QPushButton("Open Folder")
        self.open_folder_btn.setObjectName("AccentBlueButton")
        self.delete_btn = QPushButton("Delete Template")
        self.delete_btn.setObjectName("DangerButton")
        bar.addWidget(self.new_btn)
        bar.addWidget(self.save_btn)
        bar.addStretch(1)
        bar.addWidget(self.open_folder_btn)
        bar.addWidget(self.delete_btn)
        root.addLayout(bar)

        item_bar = QHBoxLayout()
        self.template_combo = CyclicComboBox()
        self.item_doc = QLineEdit()
        self.item_desc = QLineEdit()
        item_bar.addWidget(QLabel("Template"))
        item_bar.addWidget(self.template_combo, 1)
        item_bar.addWidget(QLabel("Document Name"))
        item_bar.addWidget(self.item_doc, 2)
        item_bar.addWidget(QLabel("Description"))
        item_bar.addWidget(self.item_desc, 2)
        root.addLayout(item_bar)

        self.item_tbl = QTableWidget(0, 1)
        self.item_tbl.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.item_tbl.setVisible(False)

        foot = QHBoxLayout()
        foot.setSpacing(16)
        self.add_item_btn = QPushButton("Add")
        self.add_item_btn.setObjectName("PrimaryButton")
        self.update_item_btn = QPushButton("Update")
        self.update_item_btn.setObjectName("AccentBlueButton")
        self.attach_btn = QPushButton("Attach")
        attach_icon = QIcon.fromTheme("mail-attachment")
        if attach_icon.isNull():
            self.attach_btn.setText("Attach")
        else:
            self.attach_btn.setIcon(attach_icon)
        self.del_item_btn = QPushButton("Delete")
        for b in (self.add_item_btn, self.update_item_btn, self.attach_btn, self.del_item_btn):
            b.setMinimumWidth(136)
            b.setFixedHeight(36)
        foot.addStretch(1)
        foot.addWidget(self.add_item_btn)
        foot.addWidget(self.update_item_btn)
        foot.addWidget(self.attach_btn)
        foot.addWidget(self.del_item_btn)
        foot.addStretch(1)
        self.tbl = QTableWidget(0, 4)
        self.tbl.setHorizontalHeaderLabels(["Sr. No", "Document Name", "Description", "Attachment Name"])
        self.tbl.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tbl.verticalHeader().setVisible(False)
        self.tbl.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tbl.setSelectionMode(QAbstractItemView.SingleSelection)
        self.tbl.setWordWrap(True)
        self.tbl.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.tbl.horizontalHeader().setStretchLastSection(True)
        self.tbl.setColumnWidth(0, 90)
        self.tbl.setColumnWidth(1, 360)
        self.tbl.setColumnWidth(2, 360)
        self.tbl.installEventFilter(self)

        left_box = QFrame()
        left_layout = QVBoxLayout(left_box)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.addLayout(foot)
        left_layout.addWidget(self.tbl, 1)

        self.preview_box = QFrame()
        self.preview_box.setObjectName("PreviewBox")
        preview_layout = QVBoxLayout(self.preview_box)
        preview_layout.setContentsMargins(8, 8, 8, 8)
        preview_layout.setSpacing(6)
        preview_layout.addWidget(QLabel("Preview Area"))
        self.preview_label = QLabel("Preview Area\n(Select a file)")
        self.preview_label.setAlignment(Qt.AlignCenter)
        self.preview_label.setMinimumHeight(260)
        self.preview_label.setStyleSheet("background:#ffffff;border:1px solid #d2dce8;border-radius:8px;")
        self.preview_label.installEventFilter(self)
        preview_layout.addWidget(self.preview_label, 1)
        nav = QHBoxLayout()
        self.prev_btn = QPushButton("<")
        self.next_btn = QPushButton(">")
        self.page_edit = QLineEdit("1")
        self.page_edit.setFixedWidth(46)
        self.page_total = QLabel("1")
        nav.addWidget(self.prev_btn)
        nav.addWidget(self.page_edit)
        nav.addWidget(QLabel("/"))
        nav.addWidget(self.page_total)
        nav.addWidget(self.next_btn)
        nav.addStretch(1)
        preview_layout.addLayout(nav)
        self.preview_box.setMinimumWidth(0)

        self.body_splitter = QSplitter(Qt.Horizontal)
        self.body_splitter.addWidget(left_box)
        self.body_splitter.addWidget(self.preview_box)
        self.body_splitter.setChildrenCollapsible(True)
        self.body_splitter.setCollapsible(1, True)
        self.body_splitter.setStretchFactor(0, 3)
        self.body_splitter.setStretchFactor(1, 2)
        self._load_splitter_sizes()
        root.addWidget(self.body_splitter, 1)
        self.preview_toggle_btn = TrapezoidToggleButton(">", self)
        self.preview_toggle_btn.clicked.connect(self.toggle_preview_column)

        self.save_btn.clicked.connect(self.save_from_project)
        self.new_btn.clicked.connect(self.create_new_template)
        self.open_folder_btn.clicked.connect(self.open_template_folder)
        self.delete_btn.clicked.connect(self.delete_selected)
        self.attach_btn.clicked.connect(self.attach_files)
        self.template_combo.currentIndexChanged.connect(self._on_template_combo_changed)
        self.tbl.itemSelectionChanged.connect(self._on_table_selection_changed)
        self.tbl.cellClicked.connect(self._on_table_cell_clicked)
        self.tbl.cellDoubleClicked.connect(self._open_attachment_from_cell)
        self.tbl.horizontalHeader().sectionResized.connect(self._on_table_section_resized)
        self.add_item_btn.clicked.connect(self.add_item_to_template)
        self.update_item_btn.clicked.connect(self.update_item_in_template)
        self.del_item_btn.clicked.connect(self.delete_item_from_template)
        self.prev_btn.clicked.connect(self.prev_page)
        self.next_btn.clicked.connect(self.next_page)
        self.body_splitter.splitterMoved.connect(self._on_splitter_moved)
        self._load_column_widths()
        QTimer.singleShot(0, self._position_preview_toggle)
        self.reload()

    def reload(self):
        self.on_template_select()

    def _settings_key_col_widths(self):
        return "templates_page_column_widths"

    def _settings_key_splitter(self):
        return "templates_page_splitter_sizes"

    def _on_table_section_resized(self, *_args):
        self._rowfit_timer.start(80)
        self._colsave_timer.start(250)

    def _fit_rows(self):
        self.tbl.resizeRowsToContents()
        for r in range(self.tbl.rowCount()):
            if self.tbl.rowHeight(r) < 24:
                self.tbl.setRowHeight(r, 24)

    def _save_column_widths(self):
        widths = []
        for c in range(self.tbl.columnCount()):
            widths.append(int(self.tbl.columnWidth(c)))
        core.set_user_setting(self._settings_key_col_widths(), widths)

    def _load_column_widths(self):
        raw = core.get_user_setting(self._settings_key_col_widths(), None)
        if not isinstance(raw, list):
            return
        for c in range(min(self.tbl.columnCount(), len(raw))):
            try:
                w = int(raw[c])
            except Exception:
                continue
            if w >= 50:
                self.tbl.setColumnWidth(c, w)

    def _save_splitter_sizes(self, *_args):
        try:
            sizes = [int(x) for x in self.body_splitter.sizes()]
            if len(sizes) >= 2 and sizes[1] <= 32:
                return
            core.set_user_setting(self._settings_key_splitter(), sizes)
        except Exception:
            pass

    def _load_splitter_sizes(self):
        # Start collapsed, but keep pane in splitter so drag/resize behavior stays intact.
        self.preview_box.setVisible(True)
        self.body_splitter.setSizes([980, 0])
        if hasattr(self, "preview_toggle_btn"):
            self.preview_toggle_btn.setText("<")

    def _on_splitter_moved(self, *_args):
        sizes = self.body_splitter.sizes()
        is_collapsed = len(sizes) >= 2 and int(sizes[1]) <= 32
        if not is_collapsed:
            self._save_splitter_sizes()
        self._position_preview_toggle()
        self.preview_toggle_btn.setText("<" if is_collapsed else ">")

    def toggle_preview_column(self):
        sizes = self.body_splitter.sizes()
        total = max(400, int(sum(sizes)) if len(sizes) >= 2 else int(self.body_splitter.width()))
        is_collapsed = (not self.preview_box.isVisible()) or (len(sizes) >= 2 and int(sizes[1]) <= 32)
        if is_collapsed:
            saved = core.get_user_setting(self._settings_key_splitter(), [980, 520])
            if isinstance(saved, list) and len(saved) >= 2:
                right = max(360, int(saved[1]))
                left = max(200, total - right)
                self.preview_box.setVisible(True)
                self.body_splitter.setSizes([left, right])
            else:
                self.preview_box.setVisible(True)
                self.body_splitter.setSizes([max(200, total - 520), 520])
            self.preview_toggle_btn.setText(">")
        else:
            self._save_splitter_sizes()
            self.body_splitter.setSizes([max(200, total - 2), 0])
            self.preview_toggle_btn.setText("<")
        self._position_preview_toggle()

    def _position_preview_toggle(self):
        if not getattr(self, "body_splitter", None):
            return
        sizes = self.body_splitter.sizes()
        if len(sizes) < 2:
            return
        bx = self.body_splitter.x() + int(sizes[0]) - (self.preview_toggle_btn.width() // 2)
        by = self.body_splitter.y() + max(8, (self.body_splitter.height() - self.preview_toggle_btn.height()) // 2)
        self.preview_toggle_btn.move(int(bx), int(by))
        self.preview_toggle_btn.raise_()

    def _selected_template_id(self):
        combo_tid = self.template_combo.currentData()
        if isinstance(combo_tid, int) and combo_tid > 0:
            return combo_tid
        if isinstance(combo_tid, str) and combo_tid.isdigit():
            return int(combo_tid)
        rows = self.tbl.selectionModel().selectedRows() if self.tbl.selectionModel() else []
        if rows:
            r = rows[0].row()
            if 0 <= r < len(self._row_meta):
                return int(self._row_meta[r]["template_id"])
        if isinstance(self.current_template_id, int) and self.current_template_id > 0:
            return self.current_template_id
        return None

    def _reload_template_combo(self, selected_tid=None):
        if selected_tid is None:
            selected_tid = self.current_template_id
        self.template_combo.blockSignals(True)
        self.template_combo.clear()
        conn = sqlite3.connect(core.DB_FILE)
        try:
            rows = conn.execute(
                "SELECT id, COALESCE(template_no,0), COALESCE(organization,''), COALESCE(template_name,'') "
                "FROM checklist_templates ORDER BY template_no, organization, template_name, id"
            ).fetchall()
        finally:
            conn.close()
        set_index = -1
        for idx, (tid, tno, org, name) in enumerate(rows):
            label = f"{int(tno)} - {str(name).strip() or 'Template'}"
            if str(org).strip():
                label = f"{label} ({str(org).strip()})"
            self.template_combo.addItem(label, int(tid))
            if selected_tid and int(tid) == int(selected_tid):
                set_index = idx
        if self.template_combo.count() > 0:
            self.template_combo.setCurrentIndex(set_index if set_index >= 0 else 0)
        self.template_combo.blockSignals(False)

    def _on_template_combo_changed(self, _index=None):
        tid = self._selected_template_id()
        self.current_template_id = int(tid) if tid else None
        if not self.current_template_id:
            return
        for r in range(self.tbl.rowCount()):
            if r < len(self._row_meta) and int(self._row_meta[r]["template_id"]) == int(self.current_template_id):
                self.tbl.selectRow(r)
                break

    def _insert_template_shell(self, meta):
        conn = sqlite3.connect(core.DB_FILE)
        try:
            c = conn.cursor()
            c.execute(
                "INSERT INTO checklist_templates (template_no, organization, template_name, description) VALUES (?,?,?,?)",
                (int(meta["template_no"]), meta["organization"], meta["template_name"], meta["description"]),
            )
            tid = int(c.lastrowid or 0)
            folder = core.ensure_template_storage_folder(meta["organization"], meta["template_name"], tid)
            c.execute("UPDATE checklist_templates SET folder_path=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", (folder, tid))
            conn.commit()
            return tid
        finally:
            conn.close()

    def create_new_template(self):
        meta = _ask_template_meta(self, default_name="Template", default_org="", default_desc="")
        if not meta:
            return
        self._insert_template_shell(meta)
        self.reload()

    def save_from_project(self):
        conn = sqlite3.connect(core.DB_FILE)
        try:
            projects = conn.execute(
                "SELECT id, COALESCE(title,''), COALESCE(description,''), COALESCE(client_name,''), COALESCE(folder_path,'') "
                "FROM projects WHERE LOWER(COALESCE(status,''))!='archived' ORDER BY title"
            ).fetchall()
        finally:
            conn.close()
        if not projects:
            QMessageBox.information(self, "Create From Project", "No active projects found.")
            return

        pick = QDialog(self)
        pick.setWindowTitle("Create From Project")
        pick.resize(980, 560)
        pick_root = QVBoxLayout(pick)
        pick_top = QHBoxLayout()
        pick_top.addWidget(QLabel("Search"))
        search_edit = QLineEdit()
        search_edit.setPlaceholderText("Type project title or client...")
        pick_top.addWidget(search_edit, 1)
        pick_root.addLayout(pick_top)
        projects_tbl = QTableWidget(0, 6)
        projects_tbl.setHorizontalHeaderLabels(["ProjectID", "Sr. No", "Project", "Description", "Client", "FolderPath"])
        projects_tbl.setEditTriggers(QAbstractItemView.NoEditTriggers)
        projects_tbl.verticalHeader().setVisible(False)
        projects_tbl.setSelectionBehavior(QAbstractItemView.SelectRows)
        projects_tbl.setSelectionMode(QAbstractItemView.SingleSelection)
        projects_tbl.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        projects_tbl.horizontalHeader().setStretchLastSection(True)
        projects_tbl.setColumnHidden(0, True)
        projects_tbl.setColumnHidden(5, True)
        projects_tbl.setColumnWidth(1, 76)
        projects_tbl.setColumnWidth(2, 300)
        projects_tbl.setColumnWidth(3, 420)
        projects_tbl.setColumnWidth(4, 300)
        pick_root.addWidget(projects_tbl, 1)
        pick_btns = QHBoxLayout()
        close_btn = QPushButton("Close")
        next_btn = QPushButton("Next")
        next_btn.setObjectName("PrimaryButton")
        pick_btns.addStretch(1)
        pick_btns.addWidget(close_btn)
        pick_btns.addWidget(next_btn)
        pick_root.addLayout(pick_btns)

        for sr_no, (pid, title, desc, client, folder_path) in enumerate(projects, 1):
            r = projects_tbl.rowCount()
            projects_tbl.insertRow(r)
            projects_tbl.setItem(r, 0, QTableWidgetItem(str(int(pid))))
            sr_item = QTableWidgetItem(str(sr_no))
            sr_item.setTextAlignment(Qt.AlignCenter)
            projects_tbl.setItem(r, 1, sr_item)
            projects_tbl.setItem(r, 2, QTableWidgetItem(str(title)))
            projects_tbl.setItem(r, 3, QTableWidgetItem(str(desc)))
            projects_tbl.setItem(r, 4, QTableWidgetItem(str(client)))
            projects_tbl.setItem(r, 5, QTableWidgetItem(str(folder_path)))
        if projects_tbl.rowCount() > 0:
            projects_tbl.selectRow(0)

        def filter_projects():
            q = str(search_edit.text() or "").strip().lower()
            for r in range(projects_tbl.rowCount()):
                p = str(projects_tbl.item(r, 2).text() if projects_tbl.item(r, 2) else "").lower()
                d = str(projects_tbl.item(r, 3).text() if projects_tbl.item(r, 3) else "").lower()
                c = str(projects_tbl.item(r, 4).text() if projects_tbl.item(r, 4) else "").lower()
                show = (q in p) or (q in d) or (q in c) if q else True
                projects_tbl.setRowHidden(r, not show)
            if projects_tbl.selectionModel() and not projects_tbl.selectionModel().selectedRows():
                for r in range(projects_tbl.rowCount()):
                    if not projects_tbl.isRowHidden(r):
                        projects_tbl.selectRow(r)
                        break

        selected_project = {"pid": 0, "title": "", "client": "", "folder_path": ""}

        def accept_project():
            rows = projects_tbl.selectionModel().selectedRows() if projects_tbl.selectionModel() else []
            if not rows:
                QMessageBox.information(pick, "Create From Project", "Select a project to continue.")
                return
            r = rows[0].row()
            selected_project["pid"] = int(str(projects_tbl.item(r, 0).text() or "0") or 0)
            selected_project["title"] = str(projects_tbl.item(r, 2).text() or "")
            selected_project["client"] = str(projects_tbl.item(r, 4).text() or "")
            selected_project["folder_path"] = str(projects_tbl.item(r, 5).text() or "")
            if selected_project["pid"] <= 0:
                QMessageBox.warning(pick, "Create From Project", "Invalid project selection.")
                return
            pick.accept()

        search_edit.textChanged.connect(filter_projects)
        close_btn.clicked.connect(pick.reject)
        next_btn.clicked.connect(accept_project)
        projects_tbl.cellDoubleClicked.connect(lambda *_args: accept_project())
        filter_projects()
        if pick.exec() != QDialog.Accepted:
            return

        pid = int(selected_project["pid"])
        title = str(selected_project["title"])
        client = str(selected_project["client"])
        p_folder = str(selected_project["folder_path"])
        project_root = core.resolve_project_folder_path(p_folder, title)
        project_root = core.ensure_project_standard_folders(project_root)["project_root"]

        files_dlg = QDialog(self)
        files_dlg.setWindowTitle(f"Create From Project - {title}")
        files_dlg.resize(1080, 660)
        files_root = QVBoxLayout(files_dlg)
        info = QLabel(f"Project: {title}" + (f" ({client})" if client else ""))
        files_root.addWidget(info)
        table = QTableWidget(0, 8)
        table.setHorizontalHeaderLabels(["Sel", "ItemID", "Folder", "Sr. No", "Document Name", "Description", "Attachment Name", "Attachment Path"])
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.verticalHeader().setVisible(False)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        table.horizontalHeader().setStretchLastSection(True)
        table.setColumnHidden(1, True)
        table.setColumnHidden(7, True)
        table.setColumnWidth(0, 56)
        table.setColumnWidth(2, 180)
        table.setColumnWidth(3, 78)
        table.setColumnWidth(4, 300)
        table.setColumnWidth(5, 260)
        table.setColumnWidth(6, 220)
        files_root.addWidget(table, 1)
        buttons = QHBoxLayout()
        create_btn = QPushButton("Create Template")
        create_btn.setObjectName("PrimaryButton")
        close2_btn = QPushButton("Close")
        buttons.addStretch(1)
        buttons.addWidget(close2_btn)
        buttons.addWidget(create_btn)
        files_root.addLayout(buttons)

        conn2 = sqlite3.connect(core.DB_FILE)
        try:
            rows = conn2.execute(
                "SELECT id, COALESCE(sr_no,0), COALESCE(req_file_name,''), COALESCE(description,''), COALESCE(linked_file_path,''), COALESCE(subfolder,'Main') "
                "FROM checklist_items WHERE project_id=? ORDER BY subfolder, sr_no, id",
                (pid,),
            ).fetchall()
        finally:
            conn2.close()

        known_paths = set()
        sr_next = 1
        for iid, sr_no, doc, desc, lp, sub in rows:
            sub_txt = str(sub or "Main").strip() or "Main"
            lp_txt = str(lp or "").strip()
            src = ""
            if lp_txt:
                if os.path.isabs(lp_txt):
                    src = lp_txt
                else:
                    src = os.path.join(project_root, lp_txt)
            if not src:
                base = project_root if sub_txt == "Main" else os.path.join(project_root, sub_txt)
                candidate = os.path.join(base, str(doc or ""))
                if os.path.isfile(candidate):
                    src = candidate
            if src:
                try:
                    known_paths.add(os.path.normcase(os.path.abspath(src)))
                except Exception:
                    pass
            r = table.rowCount()
            table.insertRow(r)
            table.setItem(r, 0, _mk_check(True))
            table.setItem(r, 1, QTableWidgetItem(str(iid)))
            table.setItem(r, 2, QTableWidgetItem(str(sub_txt)))
            table.setItem(r, 3, QTableWidgetItem(str(sr_no)))
            table.setItem(r, 4, QTableWidgetItem(str(doc)))
            table.setItem(r, 5, QTableWidgetItem(str(desc)))
            table.setItem(r, 6, QTableWidgetItem(os.path.basename(src) if src else ""))
            table.setItem(r, 7, QTableWidgetItem(str(src)))
            sr_next = max(sr_next, int(sr_no or 0) + 1)

        if os.path.isdir(project_root):
            for root_dir, _dirs, files in os.walk(project_root):
                rel = os.path.relpath(root_dir, project_root)
                sub_txt = "Main" if rel in (".", "") else rel
                for name in sorted(files):
                    full = os.path.join(root_dir, name)
                    try:
                        norm = os.path.normcase(os.path.abspath(full))
                    except Exception:
                        norm = full
                    if norm in known_paths:
                        continue
                    r = table.rowCount()
                    table.insertRow(r)
                    table.setItem(r, 0, _mk_check(True))
                    table.setItem(r, 1, QTableWidgetItem("0"))
                    table.setItem(r, 2, QTableWidgetItem(str(sub_txt)))
                    table.setItem(r, 3, QTableWidgetItem(str(sr_next)))
                    table.setItem(r, 4, QTableWidgetItem(str(name)))
                    table.setItem(r, 5, QTableWidgetItem("Imported from folder"))
                    table.setItem(r, 6, QTableWidgetItem(str(name)))
                    table.setItem(r, 7, QTableWidgetItem(str(full)))
                    sr_next += 1

        def create_template():
            meta = _ask_template_meta(self, default_name=title or "Template", default_org=client or "", default_desc="Imported from project selection")
            if not meta:
                return
            tid = self._insert_template_shell(meta)
            folder = core.ensure_template_storage_folder(meta["organization"], meta["template_name"], tid)
            conn3 = sqlite3.connect(core.DB_FILE)
            try:
                c = conn3.cursor()
                for r in range(table.rowCount()):
                    chk = table.item(r, 0)
                    if not chk or chk.checkState() != Qt.Checked:
                        continue
                    sr_no = int(str(table.item(r, 3).text() if table.item(r, 3) else "0") or 0)
                    sub = str(table.item(r, 2).text() if table.item(r, 2) else "Main").strip() or "Main"
                    doc = str(table.item(r, 4).text() if table.item(r, 4) else "")
                    desc = str(table.item(r, 5).text() if table.item(r, 5) else "")
                    src = str(table.item(r, 7).text() if table.item(r, 7) else "").strip()
                    c.execute(
                        "INSERT INTO checklist_template_items (template_id, sr_no, req_file_name, description, subfolder) VALUES (?,?,?,?,?)",
                        (tid, sr_no, doc, desc, sub),
                    )
                    item_id = int(c.lastrowid or 0)
                    if src and os.path.isfile(src):
                        item_dir = os.path.join(folder, f"item_{item_id}")
                        os.makedirs(item_dir, exist_ok=True)
                        fname = core.sanitize_name(os.path.basename(src), "attachment.bin")
                        dst = os.path.join(item_dir, fname)
                        i = 1
                        stem, ext = os.path.splitext(dst)
                        while os.path.exists(dst):
                            dst = f"{stem}_{i}{ext}"
                            i += 1
                        shutil.copy2(src, dst)
                        c.execute(
                            "INSERT INTO checklist_template_item_files (template_item_id, file_name, source_name, stored_path) VALUES (?,?,?,?)",
                            (item_id, os.path.basename(dst), os.path.basename(src), dst),
                        )
                c.execute("UPDATE checklist_templates SET updated_at=CURRENT_TIMESTAMP WHERE id=?", (tid,))
                conn3.commit()
            finally:
                conn3.close()
            files_dlg.accept()

        close2_btn.clicked.connect(files_dlg.reject)
        create_btn.clicked.connect(create_template)
        if files_dlg.exec() == QDialog.Accepted:
            self.reload()

    def on_template_select(self):
        prev = self._selected_template_id()
        self.tbl.setRowCount(0)
        self._row_meta = []
        conn = sqlite3.connect(core.DB_FILE)
        try:
            rows = conn.execute(
                """SELECT i.id, COALESCE(i.sr_no,0), COALESCE(i.req_file_name,''), COALESCE(i.description,''),
                          COALESCE(f.id,0), COALESCE(f.file_name,''), COALESCE(f.stored_path,''), t.id
                   FROM checklist_template_items i
                   JOIN checklist_templates t ON t.id=i.template_id
                   LEFT JOIN checklist_template_item_files f ON f.template_item_id=i.id
                   ORDER BY t.template_no, t.organization, t.template_name, i.sr_no, i.id, f.id""",
            ).fetchall()
        finally:
            conn.close()
        by_item = {}
        for iid, sr, doc, desc, fid, fname, fpath, tid in rows:
            rec = by_item.setdefault((tid, iid), {"tid": tid, "sr": sr, "doc": doc, "desc": desc, "files": []})
            if fname and fpath:
                rec["files"].append({"file_id": int(fid or 0), "name": str(fname), "path": str(fpath)})
        keep_row = -1
        for _key, rec in by_item.items():
            r = self.tbl.rowCount()
            self.tbl.insertRow(r)
            iid_val = int(_key[1])
            self.tbl.setItem(r, 0, QTableWidgetItem(str(rec["sr"])))
            self.tbl.setItem(r, 1, QTableWidgetItem(str(rec["doc"])))
            self.tbl.setItem(r, 2, QTableWidgetItem(str(rec["desc"])))

            files = list(rec.get("files") or [])
            if files:
                self.tbl.setCellWidget(r, 3, self._build_attachment_cell_widget(r, files))
            else:
                self.tbl.setItem(r, 3, QTableWidgetItem("-"))

            self._row_meta.append(
                {
                    "item_id": iid_val,
                    "template_id": int(rec["tid"]),
                    "files": files,
                    "paths": [str(f.get("path") or "").strip() for f in files if str(f.get("path") or "").strip()],
                }
            )
            if prev and int(rec["tid"]) == int(prev) and keep_row < 0:
                keep_row = r
        if keep_row >= 0:
            self.tbl.selectRow(keep_row)
            self.current_template_id = prev
        elif self.tbl.rowCount() > 0:
            self.tbl.selectRow(0)
            self._on_table_selection_changed()
        else:
            self.current_template_id = None
        self._reload_template_combo(self.current_template_id)
        self._fit_rows()

    def _on_table_selection_changed(self):
        tid = self._selected_template_id()
        self.current_template_id = int(tid) if tid else None
        if self.current_template_id:
            idx = self.template_combo.findData(self.current_template_id)
            if idx >= 0 and idx != self.template_combo.currentIndex():
                self.template_combo.blockSignals(True)
                self.template_combo.setCurrentIndex(idx)
                self.template_combo.blockSignals(False)
        rows = self.tbl.selectionModel().selectedRows() if self.tbl.selectionModel() else []
        if rows:
            row_idx = rows[0].row()
            self.item_doc.setText(str(self.tbl.item(row_idx, 1).text() or ""))
            self.item_desc.setText(str(self.tbl.item(row_idx, 2).text() or ""))
            self._load_preview_from_row(row_idx)
        else:
            self._load_preview_path("")

    def _on_table_cell_clicked(self, row, _col):
        if row >= 0:
            self.tbl.selectRow(row)

    def add_item_to_template(self):
        tid = self._selected_template_id()
        if not tid:
            QMessageBox.information(self, "Template", "Select a template first.")
            return
        doc = str(self.item_doc.text() or "").strip()
        desc = str(self.item_desc.text() or "").strip()
        sub = "Main"
        if not doc:
            QMessageBox.warning(self, "Template", "Document name is required.")
            return
        conn = sqlite3.connect(core.DB_FILE)
        try:
            row = conn.execute("SELECT COALESCE(MAX(sr_no),0) FROM checklist_template_items WHERE template_id=?", (tid,)).fetchone()
            sr = int((row[0] if row else 0) or 0) + 1
            conn.execute(
                "INSERT INTO checklist_template_items (template_id, sr_no, req_file_name, description, subfolder) VALUES (?,?,?,?,?)",
                (tid, sr, doc, desc, sub),
            )
            conn.execute("UPDATE checklist_templates SET updated_at=CURRENT_TIMESTAMP WHERE id=?", (tid,))
            conn.commit()
        finally:
            conn.close()
        self.item_doc.clear()
        self.item_desc.clear()
        self.on_template_select()
        self.reload()

    def update_item_in_template(self):
        rows = self.tbl.selectionModel().selectedRows()
        if not rows:
            QMessageBox.information(self, "Template", "Select a template item to update.")
            return
        row_idx = rows[0].row()
        if not (0 <= row_idx < len(self._row_meta)):
            return
        tid = int(self._row_meta[row_idx]["template_id"])
        iid = int(self._row_meta[row_idx]["item_id"])
        doc = str(self.item_doc.text() or "").strip()
        desc = str(self.item_desc.text() or "").strip()
        if not doc:
            QMessageBox.warning(self, "Template", "Document name is required.")
            return
        conn = sqlite3.connect(core.DB_FILE)
        try:
            conn.execute(
                "UPDATE checklist_template_items SET req_file_name=?, description=? WHERE id=?",
                (doc, desc, iid),
            )
            conn.execute("UPDATE checklist_templates SET updated_at=CURRENT_TIMESTAMP WHERE id=?", (tid,))
            conn.commit()
        finally:
            conn.close()
        self.on_template_select()
        self.reload()

    def delete_item_from_template(self):
        rows = self.tbl.selectionModel().selectedRows()
        if not rows:
            return
        row_idx = rows[0].row()
        if not (0 <= row_idx < len(self._row_meta)):
            return
        tid = int(self._row_meta[row_idx]["template_id"])
        iid = int(self._row_meta[row_idx]["item_id"])
        if QMessageBox.question(self, "Template", "Delete selected template item?") != QMessageBox.Yes:
            return
        conn = sqlite3.connect(core.DB_FILE)
        try:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("DELETE FROM checklist_template_items WHERE id=?", (iid,))
            conn.execute("UPDATE checklist_templates SET updated_at=CURRENT_TIMESTAMP WHERE id=?", (tid,))
            conn.commit()
        finally:
            conn.close()
        self.on_template_select()
        self.reload()

    def delete_selected(self):
        tid = self._selected_template_id()
        if not tid:
            return
        if QMessageBox.question(self, "Templates", "Delete current template and all its items?") != QMessageBox.Yes:
            return
        conn = sqlite3.connect(core.DB_FILE)
        try:
            row = conn.execute("SELECT COALESCE(folder_path,'') FROM checklist_templates WHERE id=?", (tid,)).fetchone()
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("DELETE FROM checklist_templates WHERE id=?", (tid,))
            conn.commit()
        finally:
            conn.close()
        p = str((row[0] if row else "") or "").strip()
        if p and os.path.isdir(p):
            shutil.rmtree(p, ignore_errors=True)
        self.reload()

    def open_template_folder(self):
        tid = self._selected_template_id()
        if not tid:
            folder = str(getattr(core, "TEMPLATE_LIBRARY_FOLDER", "") or "").strip()
            if not folder:
                folder = os.path.join(os.getcwd(), "Template Library")
            os.makedirs(folder, exist_ok=True)
            try:
                if os.name == "nt":
                    os.startfile(folder)
                else:
                    QDesktopServices.openUrl(QUrl.fromLocalFile(folder))
            except Exception as e:
                QMessageBox.warning(self, "Open Folder", f"Could not open folder:\n{e}")
            return
        conn = sqlite3.connect(core.DB_FILE)
        try:
            row = conn.execute(
                "SELECT COALESCE(organization,''), COALESCE(template_name,''), COALESCE(folder_path,'') "
                "FROM checklist_templates WHERE id=?",
                (tid,),
            ).fetchone()
        finally:
            conn.close()
        if not row:
            QMessageBox.warning(self, "Templates", "Template not found.")
            return
        org = str(row[0] or "").strip() or "General"
        tname = str(row[1] or "").strip() or "Template"
        folder = str(row[2] or "").strip()
        if not folder or not os.path.isdir(folder):
            folder = core.ensure_template_storage_folder(org, tname, tid)
            conn2 = sqlite3.connect(core.DB_FILE)
            try:
                conn2.execute("UPDATE checklist_templates SET folder_path=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", (folder, tid))
                conn2.commit()
            finally:
                conn2.close()
        try:
            if os.name == "nt":
                os.startfile(folder)
            else:
                QDesktopServices.openUrl(QUrl.fromLocalFile(folder))
        except Exception as e:
            QMessageBox.warning(self, "Open Folder", f"Could not open folder:\n{e}")

    def _build_attachment_cell_widget(self, row_idx, files):
        wrap = QWidget()
        lay = QVBoxLayout(wrap)
        lay.setContentsMargins(6, 3, 6, 3)
        lay.setSpacing(5)
        for fidx, f in enumerate(files):
            line = QWidget(wrap)
            h = QHBoxLayout(line)
            h.setContentsMargins(0, 0, 0, 0)
            h.setSpacing(6)
            open_btn = AttachmentChipButton(str(f.get("name") or "Attachment"))
            open_btn.setCursor(Qt.PointingHandCursor)
            open_btn.setStyleSheet(
                "QPushButton {"
                "text-align:left; padding:5px 12px; border-radius:14px;"
                "background:#eef3f8; border:1px solid #c9d4e2; color:#14324f; font-weight:500;"
                "}"
                "QPushButton:hover {background:#e6edf6; border-color:#b5c5d7;}"
                "QPushButton:pressed {background:#dbe7f3;}"
            )
            path = str(f.get("path") or "")
            open_btn.singleClicked.connect(lambda p=path: self._load_preview_path(p))
            open_btn.doubleClicked.connect(lambda p=path: self._open_attachment_path(p))
            del_btn = QToolButton()
            del_btn.setText("x")
            del_btn.setCursor(Qt.PointingHandCursor)
            del_btn.setFixedSize(20, 20)
            del_btn.setStyleSheet(
                "QToolButton {border:0; border-radius:10px; background:#e8edf3; color:#6b7787; font-weight:700;}"
                "QToolButton:hover {background:#dbe3ec; color:#4f5f73;}"
                "QToolButton:pressed {background:#cfdae6;}"
            )
            del_btn.clicked.connect(lambda _checked=False, r=row_idx, i=fidx: self._delete_attachment_file(r, i))
            h.addWidget(open_btn, 1)
            h.addWidget(del_btn, 0)
            lay.addWidget(line)
        return wrap

    def _open_attachment_path(self, file_path):
        p = str(file_path or "").strip()
        if not p or not os.path.exists(p):
            return
        QDesktopServices.openUrl(QUrl.fromLocalFile(p))

    def _delete_attachment_file(self, row_idx, file_idx):
        if not (0 <= row_idx < len(self._row_meta)):
            return
        files = list(self._row_meta[row_idx].get("files") or [])
        if not (0 <= file_idx < len(files)):
            return
        rec = files[file_idx]
        file_id = int(rec.get("file_id") or 0)
        file_path = str(rec.get("path") or "").strip()
        if file_id <= 0:
            return
        if QMessageBox.question(self, "Attachment", "Delete this attachment file?") != QMessageBox.Yes:
            return
        conn = sqlite3.connect(core.DB_FILE)
        try:
            conn.execute("DELETE FROM checklist_template_item_files WHERE id=?", (file_id,))
            conn.commit()
        finally:
            conn.close()
        if file_path and os.path.isfile(file_path):
            try:
                os.remove(file_path)
            except Exception:
                pass
        self.on_template_select()

    def _open_attachment_link(self, href):
        url = QUrl(str(href or ""))
        if not url.isValid():
            return
        QDesktopServices.openUrl(url)

    def _open_attachment_from_cell(self, row, col):
        if col != 3:
            return
        if not (0 <= row < len(self._row_meta)):
            return
        paths = self._row_meta[row].get("paths") or []
        if not paths:
            return
        p = str(paths[0] or "").strip()
        if not p or not os.path.exists(p):
            return
        QDesktopServices.openUrl(QUrl.fromLocalFile(p))

    def _close_preview_pdf(self):
        try:
            if self.current_pdf:
                self.current_pdf.close()
        except Exception:
            pass
        self.current_pdf = None

    def _load_preview_from_row(self, row_idx):
        if not (0 <= row_idx < len(self._row_meta)):
            self._load_preview_path("")
            return
        paths = self._row_meta[row_idx].get("paths") or []
        fpath = str(paths[0] or "").strip() if paths else ""
        self._load_preview_path(fpath)

    def _load_preview_path(self, fpath):
        self._close_preview_pdf()
        self.preview_label.setPixmap(QPixmap())
        self.preview_label.setText("Preview Area\n(Select a file)")
        self.page_edit.setText("1")
        self.page_total.setText("1")
        if not fpath or not os.path.exists(fpath):
            return
        ext = os.path.splitext(fpath)[1].lower()
        if ext != ".pdf":
            self.preview_label.setText(f"Preview not available for {ext} files.")
            return
        if not getattr(core, "PDF_SUPPORT", False):
            self.preview_label.setText("Install 'pymupdf' to view PDFs.")
            return
        try:
            self.current_pdf = core.fitz.open(fpath)
            self.current_page = 0
            self.page_total.setText(str(len(self.current_pdf)))
            self.show_pdf_page()
        except Exception as e:
            self.preview_label.setText(f"Error reading PDF:\n{e}")

    def show_pdf_page(self):
        if not self.current_pdf:
            return
        try:
            page = self.current_pdf.load_page(self.current_page)
            # Render to exact display size with DPI awareness to avoid blur.
            dpr = max(1.0, float(self.devicePixelRatioF()))
            target_w = max(420.0, float(self.preview_label.width() - 20) * dpr)
            target_h = max(260.0, float(self.preview_label.height() - 20) * dpr)
            zoom_x = float(target_w) / max(1.0, float(page.rect.width))
            zoom_y = float(target_h) / max(1.0, float(page.rect.height))
            zoom = max(0.5, min(zoom_x, zoom_y))
            pix = page.get_pixmap(matrix=core.fitz.Matrix(zoom, zoom), alpha=False)
            qimg = QImage(pix.samples, pix.width, pix.height, pix.stride, QImage.Format_RGB888).copy()
            pm = QPixmap.fromImage(qimg)
            pm.setDevicePixelRatio(dpr)
            self.preview_label.setText("")
            self.preview_label.setPixmap(pm)
            self.page_edit.setText(str(self.current_page + 1))
        except Exception as e:
            self.preview_label.setText(f"Error rendering page: {e}")

    def prev_page(self):
        if self.current_pdf and self.current_page > 0:
            self.current_page -= 1
            self.show_pdf_page()

    def next_page(self):
        if self.current_pdf and self.current_page < len(self.current_pdf) - 1:
            self.current_page += 1
            self.show_pdf_page()

    def eventFilter(self, obj, event):
        if obj is getattr(self, "tbl", None) and event.type() == QEvent.Resize:
            self._rowfit_timer.start(80)
            self._position_preview_toggle()
        if obj is getattr(self, "preview_label", None) and event.type() == QEvent.Resize and self.current_pdf:
            self.show_pdf_page()
        return super().eventFilter(obj, event)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._position_preview_toggle()

    def attach_files(self):
        rows = self.tbl.selectionModel().selectedRows()
        if not rows:
            QMessageBox.information(self, "Templates", "Select a template item first.")
            return
        row_idx = rows[0].row()
        if not (0 <= row_idx < len(self._row_meta)):
            return
        tid = int(self._row_meta[row_idx]["template_id"])
        item_id = int(self._row_meta[row_idx]["item_id"])
        pick, _ = QFileDialog.getOpenFileNames(self, "Select Files to Attach")
        if not pick:
            return
        conn = sqlite3.connect(core.DB_FILE)
        try:
            meta = conn.execute("SELECT COALESCE(organization,''), COALESCE(template_name,'') FROM checklist_templates WHERE id=?", (tid,)).fetchone()
            if not meta:
                return
            folder = core.ensure_template_storage_folder(meta[0], meta[1], tid)
            c = conn.cursor()
            for src in pick:
                if not os.path.isfile(src):
                    continue
                item_dir = os.path.join(folder, f"item_{item_id}")
                os.makedirs(item_dir, exist_ok=True)
                fname = core.sanitize_name(os.path.basename(src), "attachment.bin")
                dst = os.path.join(item_dir, fname)
                i = 1
                stem, ext = os.path.splitext(dst)
                while os.path.exists(dst):
                    dst = f"{stem}_{i}{ext}"
                    i += 1
                shutil.copy2(src, dst)
                c.execute(
                    "INSERT INTO checklist_template_item_files (template_item_id, file_name, source_name, stored_path) VALUES (?,?,?,?)",
                    (item_id, os.path.basename(dst), os.path.basename(src), dst),
                )
            c.execute("UPDATE checklist_templates SET updated_at=CURRENT_TIMESTAMP WHERE id=?", (tid,))
            conn.commit()
        finally:
            conn.close()
        self.reload()


