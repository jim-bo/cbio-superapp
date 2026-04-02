/* study_view.js — Dashboard logic for the Study View summary tab.
   Included into page.html via Jinja2 include at render time.
   Requires: DashboardState, Charts, widgetData, tableSearchState globals set in page.html.
*/

// ---------------------------------------------------------------------------
// Shareable link — copies the current URL (which already has ?session_id=
// set by the server-side restore or the first middleware save) to clipboard.
// ---------------------------------------------------------------------------
async function cbioShareLink(btn) {
    try {
        await navigator.clipboard.writeText(window.location.href);
        const icon = btn.querySelector('i');
        icon.className = 'fa fa-check';
        setTimeout(() => { icon.className = 'fa fa-share-alt'; }, 1500);
    } catch (_) {}
}

// ---------------------------------------------------------------------------
// Table sorting state — { tableId: { col: 'freq', dir: 'desc' } }
// ---------------------------------------------------------------------------
const tableSortState = {};

function getTableSort(tableId, defaultCol, defaultDir) {
    if (!tableSortState[tableId]) {
        tableSortState[tableId] = { col: defaultCol, dir: defaultDir || 'desc' };
    }
    return tableSortState[tableId];
}

function toggleTableSort(tableId, col, renderFn) {
    const st = getTableSort(tableId);
    if (st.col === col) {
        st.dir = st.dir === 'desc' ? 'asc' : 'desc';
    } else {
        st.col = col;
        st.dir = 'desc';
    }
    renderFn();
}

function sortIndicator(tableId, col) {
    const st = tableSortState[tableId];
    if (!st || st.col !== col) return '';
    return st.dir === 'desc' ? ' <i class="fa fa-caret-down"></i>' : ' <i class="fa fa-caret-up"></i>';
}

function sortData(data, col, dir, accessor) {
    const sorted = [...data];
    const get = accessor || (item => item[col]);
    sorted.sort((a, b) => {
        const va = get(a), vb = get(b);
        if (va == null && vb == null) return 0;
        if (va == null) return 1;
        if (vb == null) return -1;
        if (typeof va === 'string') return dir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
        return dir === 'asc' ? va - vb : vb - va;
    });
    return sorted;
}

function broadcastUpdate() {
    window.dispatchEvent(new CustomEvent('cbio-filter-changed'));
    updateNavbarSelectionCounts();
}

async function updateNavbarSelectionCounts() {
    const patientsEl = document.getElementById('navbar-selected-patients');
    const samplesEl = document.getElementById('navbar-selected-samples');
    if (!patientsEl || !samplesEl) return;

    try {
        const formData = new FormData();
        formData.append('study_id', DashboardState.studyId);
        formData.append('filter_json', JSON.stringify(DashboardState.filters));
        const response = await fetch('/study/summary/navbar-counts', { method: 'POST', body: formData });
        const json = await response.json();
        patientsEl.innerText = (json.n_patients || 0).toLocaleString();
        samplesEl.innerText = (json.n_samples || 0).toLocaleString();
    } catch (e) {}
}

function formatFreq(freq, count) {
    if (count > 0 && freq === 0) return '<0.1%';
    if (freq === 0) return '0%';
    if (freq >= 99.9) return '99.9%';
    return freq.toFixed(1) + '%';
}

function filterTableData(attrId, items, query) {
    if (!query || !query.trim()) return items;
    const q = query.trim().toLowerCase();
    if (attrId === '_mutated_genes')
        return items.filter(item => item.gene.toLowerCase().includes(q));
    if (attrId === '_cna_genes')
        return items.filter(item =>
            item.gene.toLowerCase().includes(q) ||
            item.cna_type.toLowerCase().includes(q)
        );
    if (attrId === '_sv_genes')
        return items.filter(item => item.gene.toLowerCase().includes(q));
    if (attrId === '_data_types')
        return items.filter(item => item.display_name.toLowerCase().includes(q));
    if (attrId === '_patient_treatments' || attrId === '_sample_treatments')
        return items.filter(item => item.treatment.toLowerCase().includes(q));
    // Clinical 'table' widgets
    return items.filter(item => item.value.toLowerCase().includes(q));
}

function renderTableTbody(attrId, data) {
    const tbody = document.getElementById(`table-body-${attrId}`);
    const selectAllBtn = document.getElementById(`btn-select-all-${attrId}`);
    if (!tbody) return;

    // Sort — clinical tables use 'pct' for frequency
    const st = getTableSort(attrId, 'freq', 'desc');
    const colMap = { freq: 'pct', count: 'count', value: 'value' };
    const sorted = sortData(data, colMap[st.col] || st.col, st.dir);

    // Update header sort indicators
    const thead = tbody.closest('table')?.querySelector('thead');
    if (thead) {
        thead.innerHTML = `<tr>
            <th></th>
            <th class="cbio-table-count cbio-sortable" data-sort-col="count">#${sortIndicator(attrId, 'count')}</th>
            <th class="cbio-table-freq cbio-sortable" data-sort-col="freq">Freq${sortIndicator(attrId, 'freq')}</th>
        </tr>`;
        thead.querySelectorAll('.cbio-sortable').forEach(th => {
            th.onclick = (e) => { e.stopPropagation(); toggleTableSort(attrId, th.dataset.sortCol, () => renderTableTbody(attrId, data)); };
        });
    }

    const currentFilter = DashboardState.filters.clinicalDataFilters.find(f => f.attributeId === attrId);
    const selectedValues = currentFilter ? currentFilter.values.map(v => v.value) : [];
    // Button operates on full widgetData, not the filtered subset
    if (selectAllBtn) {
        const fullData = widgetData[attrId] || [];
        const allSelected = fullData.length > 0 && fullData.every(item => selectedValues.includes(item.value));
        selectAllBtn.innerText = allSelected ? 'Deselect all' : 'Select all';
        selectAllBtn.onclick = (e) => {
            e.stopPropagation();
            toggleSelectAll(attrId, fullData, allSelected);
        };
    }
    tbody.innerHTML = '';
    sorted.forEach(item => {
        const isSelected = selectedValues.includes(item.value);
        const tr = document.createElement('tr');
        if (isSelected) tr.className = 'selected';
        tr.innerHTML = `
                <td><div class="cbio-color-swatch" style="background-color: ${item.color};"></div><span class="cbio-table-label" title="${item.value}">${item.value}</span></td>
                <td class="cbio-table-count"><div class="cbio-table-count-container"><input type="checkbox" class="cbio-table-checkbox" ${isSelected ? 'checked' : ''}><span class="cbio-table-count-value">${item.count.toLocaleString()}</span></div></td>
                <td class="cbio-table-freq">${formatFreq(item.pct, item.count)}</td>
            `;
        tr.onclick = (e) => { e.stopPropagation(); toggleFilter(attrId, item.value); };
        tbody.appendChild(tr);
    });
}

function renderGenomicTableTbody(data) {
    const tableId = '_mutated_genes';
    const tbody = document.getElementById('table-body-_mutated_genes');
    if (!tbody) return;

    const st = getTableSort(tableId, 'freq', 'desc');
    const sorted = sortData(data, st.col, st.dir);

    const thead = tbody.closest('table')?.querySelector('thead');
    if (thead) {
        thead.innerHTML = `<tr>
            <th class="cbio-sortable" style="text-align:left;padding-left:10px;" data-sort-col="gene"><i class="fa fa-filter" style="color:#ccc;margin-right:4px;"></i>Gene${sortIndicator(tableId, 'gene')}</th>
            <th class="cbio-sortable" style="text-align:right;" data-sort-col="n_mut"># Mut${sortIndicator(tableId, 'n_mut')}</th>
            <th class="cbio-sortable" style="text-align:right;" data-sort-col="n_samples">#${sortIndicator(tableId, 'n_samples')}</th>
            <th class="cbio-sortable" style="text-align:right;" data-sort-col="freq">Freq${sortIndicator(tableId, 'freq')}</th>
        </tr>`;
        thead.querySelectorAll('.cbio-sortable').forEach(th => {
            th.onclick = (e) => { e.stopPropagation(); toggleTableSort(tableId, th.dataset.sortCol, () => renderGenomicTableTbody(data)); };
        });
    }

    const selectedGenes = DashboardState.filters.mutationFilter.genes;
    tbody.innerHTML = '';
    sorted.forEach(item => {
        const isSelected = selectedGenes.includes(item.gene);
        const tr = document.createElement('tr'); if (isSelected) tr.className = 'selected';
        tr.innerHTML = `<td style="padding-left: 10px;"><span class="cbio-table-label font-bold" title="${item.gene}">${item.gene}</span></td><td style="text-align: right;">${item.n_mut.toLocaleString()}</td><td class="cbio-table-count"><div class="cbio-table-count-container"><input type="checkbox" class="cbio-table-checkbox" ${isSelected ? 'checked' : ''}><span class="cbio-table-count-value">${item.n_samples.toLocaleString()}</span></div></td><td class="cbio-table-freq">${formatFreq(item.freq, item.n_samples)}</td>`;
        tr.onclick = (e) => { e.stopPropagation(); toggleMutationFilter(item.gene); };
        tbody.appendChild(tr);
    });
}

function renderCNATableTbody(data) {
    const tableId = '_cna_genes';
    const tbody = document.getElementById('table-body-_cna_genes');
    if (!tbody) return;

    const st = getTableSort(tableId, 'freq', 'desc');
    const sorted = sortData(data, st.col, st.dir);

    const thead = tbody.closest('table')?.querySelector('thead');
    if (thead) {
        thead.innerHTML = `<tr>
            <th class="cbio-sortable" style="text-align:left;padding-left:10px;" data-sort-col="gene"><i class="fa fa-filter" style="color:#ccc;margin-right:4px;"></i>Gene${sortIndicator(tableId, 'gene')}</th>
            <th class="cbio-sortable" style="text-align:left;" data-sort-col="cytoband">Cytoband${sortIndicator(tableId, 'cytoband')}</th>
            <th class="cbio-sortable" style="text-align:center;" data-sort-col="cna_type">CNA${sortIndicator(tableId, 'cna_type')}</th>
            <th class="cbio-sortable" style="text-align:right;" data-sort-col="n_samples">#${sortIndicator(tableId, 'n_samples')}</th>
            <th class="cbio-sortable" style="text-align:right;" data-sort-col="freq">Freq${sortIndicator(tableId, 'freq')}</th>
        </tr>`;
        thead.querySelectorAll('.cbio-sortable').forEach(th => {
            th.onclick = (e) => { e.stopPropagation(); toggleTableSort(tableId, th.dataset.sortCol, () => renderCNATableTbody(data)); };
        });
    }

    const selectedGenes = DashboardState.filters.cnaFilter.genes;
    tbody.innerHTML = '';
    sorted.forEach(item => {
        const isSelected = selectedGenes.includes(item.gene);
        const tr = document.createElement('tr'); if (isSelected) tr.className = 'selected';
        tr.innerHTML = `<td style="padding-left: 10px;"><span class="cbio-table-label font-bold" title="${item.gene}">${item.gene}</span></td><td style="font-size:11px;color:#666;text-align:left;">${item.cytoband || ''}</td><td style="text-align: center;"><span style="font-weight: bold; font-size: 11px; color: ${item.cna_type === 'AMP' ? '#c00' : '#00f'};">${item.cna_type}</span></td><td class="cbio-table-count"><div class="cbio-table-count-container"><input type="checkbox" class="cbio-table-checkbox" ${isSelected ? 'checked' : ''}><span class="cbio-table-count-value">${item.n_samples.toLocaleString()}</span></div></td><td class="cbio-table-freq">${formatFreq(item.freq, item.n_samples)}</td>`;
        tr.onclick = (e) => { e.stopPropagation(); toggleCNAFilter(item.gene); };
        tbody.appendChild(tr);
    });
}

function renderSVTableTbody(data) {
    const tableId = '_sv_genes';
    const tbody = document.getElementById('table-body-_sv_genes');
    if (!tbody) return;

    const st = getTableSort(tableId, 'freq', 'desc');
    const sorted = sortData(data, st.col, st.dir);

    const thead = tbody.closest('table')?.querySelector('thead');
    if (thead) {
        thead.innerHTML = `<tr>
            <th class="cbio-sortable" style="text-align:left;padding-left:10px;" data-sort-col="gene"><i class="fa fa-filter" style="color:#ccc;margin-right:4px;"></i>Gene${sortIndicator(tableId, 'gene')}</th>
            <th class="cbio-sortable" style="text-align:right;" data-sort-col="n_sv"># SV${sortIndicator(tableId, 'n_sv')}</th>
            <th class="cbio-sortable" style="text-align:right;" data-sort-col="n_samples">#${sortIndicator(tableId, 'n_samples')}</th>
            <th class="cbio-sortable" style="text-align:right;" data-sort-col="freq">Freq${sortIndicator(tableId, 'freq')}</th>
        </tr>`;
        thead.querySelectorAll('.cbio-sortable').forEach(th => {
            th.onclick = (e) => { e.stopPropagation(); toggleTableSort(tableId, th.dataset.sortCol, () => renderSVTableTbody(data)); };
        });
    }

    const selectedGenes = DashboardState.filters.svFilter.genes;
    tbody.innerHTML = '';
    sorted.forEach(item => {
        const isSelected = selectedGenes.includes(item.gene);
        const tr = document.createElement('tr'); if (isSelected) tr.className = 'selected';
        tr.innerHTML = `<td style="padding-left: 10px;"><span class="cbio-table-label font-bold" title="${item.gene}">${item.gene}</span></td><td style="text-align: right;">${item.n_sv.toLocaleString()}</td><td class="cbio-table-count"><div class="cbio-table-count-container"><input type="checkbox" class="cbio-table-checkbox" ${isSelected ? 'checked' : ''}><span class="cbio-table-count-value">${item.n_samples.toLocaleString()}</span></div></td><td class="cbio-table-freq">${formatFreq(item.freq, item.n_samples)}</td>`;
        tr.onclick = (e) => { e.stopPropagation(); toggleSVFilter(item.gene); };
        tbody.appendChild(tr);
    });
}

function renderDataTypesTbody(data) {
    const tbody = document.getElementById('table-body-_data_types');
    if (!tbody) return;
    tbody.innerHTML = '';
    data.forEach(item => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
                <td style="padding-left:10px;">${item.display_name}</td>
                <td style="text-align:right;">${item.count.toLocaleString()}</td>
                <td style="text-align:right;">${formatFreq(item.freq, item.count)}</td>`;
        tbody.appendChild(tr);
    });
}

function renderPatientTreatmentsTbody(data) {
    const tbody = document.getElementById('table-body-_patient_treatments');
    if (!tbody) return;
    tbody.innerHTML = '';
    data.forEach(item => {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td style="padding-left:10px;">${item.treatment}</td><td style="text-align:right;">${item.count.toLocaleString()}</td>`;
        tbody.appendChild(tr);
    });
}

function renderSampleTreatmentsTbody(data) {
    const tbody = document.getElementById('table-body-_sample_treatments');
    if (!tbody) return;
    tbody.innerHTML = '';
    data.forEach(item => {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td style="padding-left:10px;">${item.treatment}</td><td style="text-align:center;">${item.time}</td><td style="text-align:right;">${item.count.toLocaleString()}</td>`;
        tbody.appendChild(tr);
    });
}

function reRenderTableTbody(attrId) {
    const data = widgetData[attrId];
    if (!data) return;
    const filtered = filterTableData(attrId, data, tableSearchState[attrId]);
    if (attrId === '_mutated_genes') { renderGenomicTableTbody(filtered); return; }
    if (attrId === '_cna_genes')     { renderCNATableTbody(filtered);     return; }
    if (attrId === '_sv_genes')             { renderSVTableTbody(filtered);             return; }
    if (attrId === '_data_types')           { renderDataTypesTbody(filtered);           return; }
    if (attrId === '_patient_treatments')   { renderPatientTreatmentsTbody(filtered);   return; }
    if (attrId === '_sample_treatments')    { renderSampleTreatmentsTbody(filtered);    return; }
    renderTableTbody(attrId, filtered);
}

function getFiltersForWidget(excludeAttrId) {
    let f = JSON.parse(JSON.stringify(DashboardState.filters));
    if (excludeAttrId === '_mutated_genes') f.mutationFilter.genes = [];
    else if (excludeAttrId === '_sv_genes') f.svFilter.genes = [];
    else if (excludeAttrId === '_cna_genes') f.cnaFilter.genes = [];
    else f.clinicalDataFilters = f.clinicalDataFilters.filter(x => x.attributeId !== excludeAttrId);
    return f;
}


async function updateTableWidget(attrId) {
    const widget = document.getElementById(`widget-${attrId}`);
    const tbody = document.getElementById(`table-body-${attrId}`);
    if (!tbody) return;
    const loader = document.createElement('div'); loader.className = 'loading-overlay'; loader.innerText = '...';
    widget.querySelector('.cbio-widget-content').appendChild(loader);
    try {
        const formData = new FormData();
        formData.append('study_id', DashboardState.studyId);
        formData.append('attribute_id', attrId);
        formData.append('filter_json', JSON.stringify(getFiltersForWidget(attrId)));
        const response = await fetch('/study/summary/chart/clinical?format=json', { method: 'POST', body: formData });
        const json = await response.json();
        const data = json.data || [];
        widgetData[attrId] = data;
        const filtered = filterTableData(attrId, data, tableSearchState[attrId]);
        renderTableTbody(attrId, filtered);
    } finally { loader.remove(); }
}

function toggleSelectAll(attrId, data, shouldDeselect) {
    if (shouldDeselect) {
        DashboardState.filters.clinicalDataFilters = DashboardState.filters.clinicalDataFilters.filter(f => f.attributeId !== attrId);
    } else {
        const values = data.map(item => ({ value: item.value }));
        let cf = DashboardState.filters.clinicalDataFilters.find(f => f.attributeId === attrId);
        if (cf) cf.values = values;
        else DashboardState.filters.clinicalDataFilters.push({ attributeId: attrId, values: values });
    }
    broadcastUpdate();
}

async function updatePieWidget(attrId) {
    const chartDom = document.getElementById(`chart-${attrId}`);
    if (!chartDom) return;
    if (!Charts.Pies[attrId]) Charts.Pies[attrId] = echarts.init(chartDom, null, { renderer: 'svg' });
    const chart = Charts.Pies[attrId];
    try {
        const formData = new FormData();
        formData.append('study_id', DashboardState.studyId);
        formData.append('attribute_id', attrId);
        formData.append('filter_json', JSON.stringify(getFiltersForWidget(attrId)));
        const response = await fetch('/study/summary/chart/clinical?format=json', { method: 'POST', body: formData });
        const json = await response.json();
        const data = json.data || [];
        widgetData[attrId] = data;
        const currentFilter = DashboardState.filters.clinicalDataFilters.find(f => f.attributeId === attrId);
        const selectedValues = currentFilter ? currentFilter.values.map(v => v.value) : [];
        const total = data.reduce((s, d) => s + d.count, 0);
        chart.setOption({
            tooltip: { show: false },
            series: [{
                type: 'pie', radius: '70%', center: ['50%', '50%'],
                data: data.map(item => ({
                    value: item.count, name: item.value,
                    itemStyle: { color: item.color, opacity: (selectedValues.length === 0 || selectedValues.includes(item.value)) ? 1 : 0.4, borderWidth: selectedValues.includes(item.value) ? 2 : 0, borderColor: '#333' }
                })),
                // Show count label inside slices >= 25% of total (matches legacy threshold)
                label: {
                    show: true,
                    position: 'inside',
                    formatter: (params) => {
                        if (params.percent < 25) return '';
                        const n = params.value;
                        return n >= 1000 ? `${(n / 1000).toFixed(1)}K` : `${n}`;
                    },
                    color: '#fff',
                    fontSize: 11,
                    fontWeight: 'bold'
                },
                labelLine: { show: false }
            }]
        });
        chart.off('click'); chart.on('click', (p) => toggleFilter(attrId, p.name));
        chart.off('mouseover'); chart.on('mouseover', (p) => showPieHoverTable(attrId, p.name));
        chart.off('mouseout'); chart.on('mouseout', () => schedulePieHoverTableHide());
    } catch (err) {}
}

const AGE_ATTRS = new Set(['AGE', 'CURRENT_AGE_DEID', 'DIAGNOSIS_AGE', 'AGE_AT_DIAGNOSIS']);

async function updateBarWidget(attrId) {
    const chartDom = document.getElementById(`chart-${attrId}`);
    if (!chartDom) return;
    if (!Charts.Bars[attrId]) Charts.Bars[attrId] = echarts.init(chartDom, null, { renderer: 'svg' });
    const chart = Charts.Bars[attrId];
    const naEl = document.getElementById(`na-count-${attrId}`);
    try {
        const formData = new FormData();
        formData.append('study_id', DashboardState.studyId);
        formData.append('filter_json', JSON.stringify(DashboardState.filters));
        formData.append('attribute_id', attrId);
        if (AGE_ATTRS.has(attrId)) {
            formData.append('bin_size', '5');
            formData.append('clip_min', '35');
            formData.append('clip_max', '85');
        }
        let bins, naCount = 0;
        const response = await fetch('/study/summary/chart/numeric', { method: 'POST', body: formData });
        const json = await response.json();
        bins = json.data || [];
        naCount = json.na_count || 0;
        widgetData[attrId] = bins;
        if (naEl) {
            naEl.textContent = naCount > 0 ? `NA: ${naCount}` : '';
            naEl.style.display = naCount > 0 ? 'inline' : 'none';
        }
        chart.setOption({
            tooltip: { trigger: 'axis', formatter: (params) => `${params[0].name}: ${params[0].value.toLocaleString()}` },
            xAxis: { type: 'category', data: bins.map(d => d.x), axisLabel: { rotate: -45, fontSize: 9 } },
            yAxis: { type: 'value', axisLabel: { fontSize: 9, formatter: (v) => v >= 1000 ? `${v/1000}k` : v } },
            series: [{ type: 'bar', data: bins.map(d => d.y), itemStyle: { color: '#2986E2' }, barMaxWidth: 20 }],
            grid: { left: 45, right: 10, top: 10, bottom: 55 }
        });
    } catch (e) {}
}

async function updateGenomicTableWidget() {
    const tbody = document.getElementById('table-body-_mutated_genes');
    const titleEl = document.getElementById('title-_mutated_genes');
    if (!tbody) return;
    try {
        const formData = new FormData(); formData.append('study_id', DashboardState.studyId); formData.append('filter_json', JSON.stringify(getFiltersForWidget('_mutated_genes')));
        const response = await fetch('/study/summary/chart/mutated-genes?format=json', { method: 'POST', body: formData });
        const data = await response.json() || [];
        widgetData['_mutated_genes'] = data;
        if (titleEl) titleEl.innerText = `Mutated Genes (${DashboardState.nSamples} profiled samples)`;
        const filtered = filterTableData('_mutated_genes', data, tableSearchState['_mutated_genes']);
        renderGenomicTableTbody(filtered);
    } catch (e) {}
}

async function updateCNATableWidget() {
    const tbody = document.getElementById('table-body-_cna_genes');
    const titleEl = document.getElementById('title-_cna_genes');
    if (!tbody) return;
    try {
        const formData = new FormData(); formData.append('study_id', DashboardState.studyId); formData.append('filter_json', JSON.stringify(getFiltersForWidget('_cna_genes')));
        const response = await fetch('/study/summary/chart/cna-genes?format=json', { method: 'POST', body: formData });
        const data = await response.json() || [];
        widgetData['_cna_genes'] = data;
        if (titleEl) titleEl.innerText = `CNA Genes (${DashboardState.nSamples} profiled samples)`;
        const filtered = filterTableData('_cna_genes', data, tableSearchState['_cna_genes']);
        renderCNATableTbody(filtered);
    } catch (e) {}
}

async function updateSVTableWidget() {
    const tbody = document.getElementById('table-body-_sv_genes');
    const titleEl = document.getElementById('title-_sv_genes');
    if (!tbody) return;
    try {
        const formData = new FormData(); formData.append('study_id', DashboardState.studyId); formData.append('filter_json', JSON.stringify(getFiltersForWidget('_sv_genes')));
        const response = await fetch('/study/summary/chart/sv-genes?format=json', { method: 'POST', body: formData });
        const data = await response.json() || [];
        widgetData['_sv_genes'] = data;
        if (titleEl) titleEl.innerText = `Structural Variant Genes (${DashboardState.nSamples} profiled samples)`;
        const filtered = filterTableData('_sv_genes', data, tableSearchState['_sv_genes']);
        renderSVTableTbody(filtered);
    } catch (e) {}
}

async function updateScatterWidget() {
    const chartDom = document.getElementById('chart-_scatter');
    if (!chartDom) return;
    if (!Charts.Scatter) Charts.Scatter = echarts.init(chartDom, null, { renderer: 'svg' });
    try {
        const formData = new FormData();
        formData.append('study_id', DashboardState.studyId);
        formData.append('filter_json', JSON.stringify(DashboardState.filters));
        const response = await fetch('/study/summary/chart/scatter?format=json', { method: 'POST', body: formData });
        const data = await response.json();

        const PLASMA = [
            '#0d0887','#46039f','#7201a8','#9c179e','#bd3786',
            '#d8576b','#ed7953','#fb9f3a','#fdb42f'
        ];

        const logMin = Math.log(Math.max(1, data.count_min));
        const logMax = Math.log(Math.max(1, data.count_max));
        const toCoord = c => (logMax === logMin) ? 0.375
            : 0.75 * (Math.log(c) - logMin) / (logMax - logMin);
        const midCount = Math.round(Math.exp(0.375 * (logMax - logMin) / 0.75 + logMin));

        const gridLeft = 65, gridRight = 85;
        const plotWidth = chartDom.offsetWidth - gridLeft - gridRight;
        const symbolSize = Math.max(4, Math.floor(plotWidth / 40));

        const seriesData = data.bins.map(b => [
            b.bin_x + data.x_bin_size / 2,
            b.bin_y + data.y_bin_size / 2,
            b.count,
            toCoord(b.count),
        ]);

        Charts.Scatter.setOption({
            grid: { left: 65, right: 85, top: 30, bottom: 45 },
            xAxis: {
                type: 'value', min: 0, max: 1,
                name: 'Fraction Genome Altered',
                nameLocation: 'middle', nameGap: 30,
                axisLabel: { fontSize: 10 },
                splitLine: { lineStyle: { color: '#eee' } }
            },
            yAxis: {
                type: 'value', min: 0,
                name: 'Mutation Count',
                nameLocation: 'middle', nameGap: 45,
                axisLabel: { fontSize: 10 },
                splitLine: { lineStyle: { color: '#eee' } }
            },
            visualMap: {
                type: 'continuous',
                dimension: 3, min: 0, max: 0.75,
                orient: 'vertical',
                right: 5, top: 70,
                itemWidth: 10, itemHeight: 80,
                text: [data.count_max.toLocaleString(), '1'],
                textStyle: { fontSize: 10 },
                inRange: { color: PLASMA },
                show: true
            },
            series: [{
                type: 'scatter',
                data: seriesData,
                symbolSize: symbolSize,
                encode: { x: 0, y: 1, tooltip: 2 },
            }],
            graphic: [
                { type: 'text', right: 10, top: 55,
                  style: { text: '# samples', fontSize: 10, fill: '#555' } },
                { type: 'text', right: 5, top: 107,
                  style: { text: midCount.toLocaleString(), fontSize: 10, fill: '#555' } },
                { type: 'text', right: 5, top: 175,
                  style: { text: 'Pearson:', fontSize: 10, fill: '#555', fontWeight: 'bold' } },
                { type: 'text', right: 5, top: 188,
                  style: { text: data.pearson_corr.toFixed(4), fontSize: 10, fill: '#555' } },
                { type: 'text', right: 5, top: 201,
                  style: { text: 'p=' + data.pearson_pval.toFixed(2), fontSize: 10, fill: '#555' } },
                { type: 'text', right: 5, top: 220,
                  style: { text: 'Spearman:', fontSize: 10, fill: '#555', fontWeight: 'bold' } },
                { type: 'text', right: 5, top: 233,
                  style: { text: data.spearman_corr.toFixed(4), fontSize: 10, fill: '#555' } },
                { type: 'text', right: 5, top: 246,
                  style: { text: 'p=' + data.spearman_pval.toFixed(2), fontSize: 10, fill: '#555' } },
            ],
            tooltip: {
                trigger: 'item',
                formatter: p => `${p.data[2]} samples<br/>FGA ≈ ${p.data[0].toFixed(3)}<br/>Mut ≈ ${Math.round(p.data[1])}`
            }
        });
    } catch (e) {}
}

async function updateDataTypesWidget() {
    const tbody = document.getElementById('table-body-_data_types');
    if (!tbody) return;
    try {
        const formData = new FormData();
        formData.append('study_id', DashboardState.studyId);
        formData.append('filter_json', JSON.stringify(DashboardState.filters));
        const response = await fetch('/study/summary/chart/data-types?format=json', { method: 'POST', body: formData });
        const data = await response.json() || [];
        widgetData['_data_types'] = data;
        const filtered = filterTableData('_data_types', data, tableSearchState['_data_types']);
        renderDataTypesTbody(filtered);
    } catch (e) {}
}

async function updatePatientTreatmentsWidget() {
    const tbody = document.getElementById('table-body-_patient_treatments');
    if (!tbody) return;
    try {
        const formData = new FormData();
        formData.append('study_id', DashboardState.studyId);
        formData.append('filter_json', JSON.stringify(DashboardState.filters));
        const response = await fetch('/study/summary/chart/patient-treatments?format=json', { method: 'POST', body: formData });
        const json = await response.json();
        const data = json.rows || [];
        widgetData['_patient_treatments'] = data;
        const filtered = filterTableData('_patient_treatments', data, tableSearchState['_patient_treatments']);
        renderPatientTreatmentsTbody(filtered);
    } catch (e) {}
}

async function updateSampleTreatmentsWidget() {
    const tbody = document.getElementById('table-body-_sample_treatments');
    if (!tbody) return;
    try {
        const formData = new FormData();
        formData.append('study_id', DashboardState.studyId);
        formData.append('filter_json', JSON.stringify(DashboardState.filters));
        const response = await fetch('/study/summary/chart/sample-treatments?format=json', { method: 'POST', body: formData });
        const json = await response.json();
        const data = json.rows || [];
        widgetData['_sample_treatments'] = data;
        const filtered = filterTableData('_sample_treatments', data, tableSearchState['_sample_treatments']);
        renderSampleTreatmentsTbody(filtered);
    } catch (e) {}
}

async function updateKMWidget() {
    const chartDom = document.getElementById('chart-_km');
    if (!chartDom) return;
    if (!Charts.KM) Charts.KM = echarts.init(chartDom, null, { renderer: 'svg' });
    try {
        const formData = new FormData();
        formData.append('study_id', DashboardState.studyId);
        formData.append('filter_json', JSON.stringify(DashboardState.filters));
        const response = await fetch('/study/summary/chart/km?format=json', { method: 'POST', body: formData });
        const data = await response.json();
        Charts.KM.setOption({
            tooltip: { trigger: 'axis' },
            xAxis: { type: 'value', name: 'Months', axisLabel: { fontSize: 9 } },
            yAxis: { type: 'value', min: 0, max: 1, name: 'Survival', axisLabel: { fontSize: 9 } },
            series: [{ type: 'line', data: data.map(d => [d.time, d.survival]), step: 'end', itemStyle: { color: '#2986E2' }, symbol: 'none' }],
            grid: { left: 50, right: 10, top: 20, bottom: 30 }
        });
    } catch (e) {}
}

function toggleFilter(attrId, value) {
    let cf = DashboardState.filters.clinicalDataFilters.find(f => f.attributeId === attrId);
    if (!cf) { DashboardState.filters.clinicalDataFilters.push({ attributeId: attrId, values: [{ value }] }); }
    else {
        const idx = cf.values.findIndex(v => v.value === value);
        if (idx > -1) { cf.values.splice(idx, 1); if (cf.values.length === 0) DashboardState.filters.clinicalDataFilters = DashboardState.filters.clinicalDataFilters.filter(f => f.attributeId !== attrId); }
        else { cf.values.push({ value }); }
    }
    broadcastUpdate();
}

function toggleMutationFilter(gene) {
    const idx = DashboardState.filters.mutationFilter.genes.indexOf(gene);
    if (idx > -1) DashboardState.filters.mutationFilter.genes.splice(idx, 1);
    else DashboardState.filters.mutationFilter.genes.push(gene);
    broadcastUpdate();
}

function toggleSVFilter(gene) {
    const idx = DashboardState.filters.svFilter.genes.indexOf(gene);
    if (idx > -1) DashboardState.filters.svFilter.genes.splice(idx, 1);
    else DashboardState.filters.svFilter.genes.push(gene);
    broadcastUpdate();
}

function toggleCNAFilter(gene) {
    const idx = DashboardState.filters.cnaFilter.genes.indexOf(gene);
    if (idx > -1) DashboardState.filters.cnaFilter.genes.splice(idx, 1);
    else DashboardState.filters.cnaFilter.genes.push(gene);
    broadcastUpdate();
}

function routeUpdateWidget(chart) {
    const widgetEl = document.getElementById(`widget-${chart.attr_id}`);
    const viewMode = widgetEl ? widgetEl.dataset.viewMode : chart.chart_type;
    // Respect toggled view mode for table and bar widgets
    if (viewMode === 'pie' && (chart.chart_type === 'table' || chart.chart_type === 'bar')) {
        return updatePieWidget(chart.attr_id);
    }
    switch (chart.chart_type) {
        case 'pie':             return updatePieWidget(chart.attr_id);
        case 'bar':             return updateBarWidget(chart.attr_id);
        case 'table':           return updateTableWidget(chart.attr_id);
        case '_mutated_genes':  return updateGenomicTableWidget();
        case '_cna_genes':      return updateCNATableWidget();
        case '_sv_genes':       return updateSVTableWidget();
        case '_scatter':             return updateScatterWidget();
        case '_km':                  return updateKMWidget();
        case '_data_types':          return updateDataTypesWidget();
        case '_patient_treatments':  return updatePatientTreatmentsWidget();
        case '_sample_treatments':   return updateSampleTreatmentsWidget();
    }
}

function updateAll() {
    for (const chart of DashboardState.chartsMeta) {
        routeUpdateWidget(chart);
    }
}

function buildWidgetHTML(chart) {
    const { attr_id, display_name, chart_type } = chart;
    const desc = (chart.description || '').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const dn = (display_name || attr_id).replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

    function controls() {
        return `<div class="cbio-widget-controls">
            <button class="cbio-widget-btn" data-action="info-widget" title="Info"><i class="fa fa-info-circle"></i></button>
            <button class="cbio-widget-btn" data-action="close-widget" title="Close"><i class="fa fa-times"></i></button>
            <button class="cbio-widget-btn" data-action="menu-widget" title="Options"><i class="fa fa-bars"></i></button>
        </div>`;
    }

    const dataAttrs = `data-attr-id="${attr_id}" data-description="${desc}" data-chart-type="${chart_type}"`;

    if (chart_type === 'pie') {
        return `
            <div class="cbio-widget" id="widget-${attr_id}" ${dataAttrs} data-view-mode="pie">
                <div class="cbio-widget-header">
                    <div class="cbio-widget-title">${dn}</div>
                    ${controls()}
                </div>
                <div class="cbio-widget-content"><div id="chart-${attr_id}" class="echarts-container"></div></div>
            </div>`;
    }

    if (chart_type === 'bar') {
        return `
            <div class="cbio-widget" id="widget-${attr_id}" ${dataAttrs} data-view-mode="bar">
                <div class="cbio-widget-header">
                    <div class="cbio-widget-title">${dn}</div>
                    <div class="cbio-widget-controls" style="display:flex;align-items:center;gap:4px;">
                        <span id="na-count-${attr_id}" style="font-size:10px;color:#999;display:none;"></span>
                        <button class="cbio-widget-btn" data-action="info-widget" title="Info"><i class="fa fa-info-circle"></i></button>
                        <button class="cbio-widget-btn" data-action="close-widget" title="Close"><i class="fa fa-times"></i></button>
                        <button class="cbio-widget-btn" data-action="menu-widget" title="Options"><i class="fa fa-bars"></i></button>
                    </div>
                </div>
                <div class="cbio-widget-content"><div id="chart-${attr_id}" class="echarts-container"></div></div>
            </div>`;
    }

    if (chart_type === 'table') {
        return `
            <div class="cbio-widget" id="widget-${attr_id}" ${dataAttrs} data-view-mode="table">
                <div class="cbio-widget-header">
                    <div class="cbio-widget-title" id="title-${attr_id}">${dn}</div>
                    ${controls()}
                </div>
                <div class="cbio-widget-content">
                    <div id="chart-${attr_id}" class="echarts-container" style="display:none;height:100%;width:100%;"></div>
                    <table class="cbio-table">
                        <thead><tr>
                            <th></th>
                            <th class="cbio-table-count">#</th>
                            <th class="cbio-table-freq">Freq <i class="fa fa-caret-down"></i></th>
                        </tr></thead>
                        <tbody id="table-body-${attr_id}"></tbody>
                    </table>
                </div>
                <div class="cbio-widget-footer">
                    <input type="text" class="cbio-search-input" placeholder="Search...">
                    <button class="cbio-footer-btn" id="btn-select-all-${attr_id}">Select all</button>
                </div>
                <div class="cbio-resize-handle"></div>
            </div>`;
    }

    if (chart_type === '_mutated_genes') {
        return `
            <div class="cbio-widget" id="widget-_mutated_genes" ${dataAttrs} data-view-mode="_mutated_genes">
                <div class="cbio-widget-header">
                    <div class="cbio-widget-title" id="title-_mutated_genes">Mutated Genes</div>
                    ${controls()}
                </div>
                <div class="cbio-widget-content">
                    <table class="cbio-table">
                        <thead><tr>
                            <th style="text-align: left; padding-left: 10px;"><i class="fa fa-filter" style="color: #ccc; margin-right: 4px;"></i>Gene</th>
                            <th style="text-align: right;"># Mut</th>
                            <th style="text-align: right;">#</th>
                            <th style="text-align: right;">Freq <i class="fa fa-caret-down"></i></th>
                        </tr></thead>
                        <tbody id="table-body-_mutated_genes"></tbody>
                    </table>
                </div>
                <div class="cbio-widget-footer"><input type="text" class="cbio-search-input" placeholder="Search..."></div>
                <div class="cbio-resize-handle"></div>
            </div>`;
    }

    if (chart_type === '_cna_genes') {
        return `
            <div class="cbio-widget" id="widget-_cna_genes" ${dataAttrs} data-view-mode="_cna_genes">
                <div class="cbio-widget-header">
                    <div class="cbio-widget-title" id="title-_cna_genes">CNA Genes</div>
                    ${controls()}
                </div>
                <div class="cbio-widget-content">
                    <table class="cbio-table">
                        <thead><tr>
                            <th style="text-align: left; padding-left: 10px;"><i class="fa fa-filter" style="color: #ccc; margin-right: 4px;"></i>Gene</th>
                            <th>Cytoband</th>
                            <th style="text-align: center;">CNA</th>
                            <th style="text-align: right;">#</th>
                            <th style="text-align: right;">Freq <i class="fa fa-caret-down"></i></th>
                        </tr></thead>
                        <tbody id="table-body-_cna_genes"></tbody>
                    </table>
                </div>
                <div class="cbio-widget-footer"><input type="text" class="cbio-search-input" placeholder="Search..."></div>
                <div class="cbio-resize-handle"></div>
            </div>`;
    }

    if (chart_type === '_sv_genes') {
        return `
            <div class="cbio-widget" id="widget-_sv_genes" ${dataAttrs} data-view-mode="_sv_genes">
                <div class="cbio-widget-header">
                    <div class="cbio-widget-title" id="title-_sv_genes">Structural Variant Genes</div>
                    ${controls()}
                </div>
                <div class="cbio-widget-content">
                    <table class="cbio-table">
                        <thead><tr>
                            <th style="text-align: left; padding-left: 10px;"><i class="fa fa-filter" style="color: #ccc; margin-right: 4px;"></i>Gene</th>
                            <th style="text-align: right;"># SV</th>
                            <th style="text-align: right;">#</th>
                            <th style="text-align: right;">Freq <i class="fa fa-caret-down"></i></th>
                        </tr></thead>
                        <tbody id="table-body-_sv_genes"></tbody>
                    </table>
                </div>
                <div class="cbio-widget-footer"><input type="text" class="cbio-search-input" placeholder="Search..."></div>
                <div class="cbio-resize-handle"></div>
            </div>`;
    }

    if (chart_type === '_scatter') {
        return `
            <div class="cbio-widget" id="widget-_scatter" ${dataAttrs} data-view-mode="_scatter">
                <div class="cbio-widget-header">
                    <div class="cbio-widget-title">TMB vs FGA</div>
                    ${controls()}
                </div>
                <div class="cbio-widget-content"><div id="chart-_scatter" class="echarts-container"></div></div>
            </div>`;
    }

    if (chart_type === '_km') {
        return `
            <div class="cbio-widget" id="widget-_km" ${dataAttrs} data-view-mode="_km">
                <div class="cbio-widget-header">
                    <div class="cbio-widget-title">Overall Survival</div>
                    ${controls()}
                </div>
                <div class="cbio-widget-content"><div id="chart-_km" class="echarts-container"></div></div>
            </div>`;
    }

    if (chart_type === '_data_types') {
        return `
            <div class="cbio-widget" id="widget-_data_types" ${dataAttrs} data-view-mode="_data_types">
                <div class="cbio-widget-header">
                    <div class="cbio-widget-title">Data Types</div>
                    ${controls()}
                </div>
                <div class="cbio-widget-content">
                    <table class="cbio-table">
                        <thead><tr>
                            <th style="text-align:left;padding-left:10px;"></th>
                            <th style="text-align:right;">#</th>
                            <th style="text-align:right;">Freq <i class="fa fa-caret-down"></i></th>
                        </tr></thead>
                        <tbody id="table-body-_data_types"></tbody>
                    </table>
                </div>
                <div class="cbio-widget-footer"><input type="text" class="cbio-search-input" placeholder="Search..."></div>
                <div class="cbio-resize-handle"></div>
            </div>`;
    }

    if (chart_type === '_patient_treatments') {
        return `
            <div class="cbio-widget" id="widget-_patient_treatments" ${dataAttrs} data-view-mode="_patient_treatments">
                <div class="cbio-widget-header">
                    <div class="cbio-widget-title" id="title-_patient_treatments">Treatment per Patient</div>
                    ${controls()}
                </div>
                <div class="cbio-widget-content">
                    <table class="cbio-table">
                        <thead><tr>
                            <th style="text-align:left;padding-left:10px;">Treatment</th>
                            <th style="text-align:right;"># Patients</th>
                        </tr></thead>
                        <tbody id="table-body-_patient_treatments"></tbody>
                    </table>
                </div>
                <div class="cbio-widget-footer"><input type="text" class="cbio-search-input" placeholder="Search..."></div>
                <div class="cbio-resize-handle"></div>
            </div>`;
    }

    if (chart_type === '_sample_treatments') {
        return `
            <div class="cbio-widget" id="widget-_sample_treatments" ${dataAttrs} data-view-mode="_sample_treatments">
                <div class="cbio-widget-header">
                    <div class="cbio-widget-title" id="title-_sample_treatments">Treatment per Sample (pre/post)</div>
                    ${controls()}
                </div>
                <div class="cbio-widget-content">
                    <table class="cbio-table">
                        <thead><tr>
                            <th style="text-align:left;padding-left:10px;">Treatment</th>
                            <th style="text-align:center;">Pre/Post</th>
                            <th style="text-align:right;"># Samples</th>
                        </tr></thead>
                        <tbody id="table-body-_sample_treatments"></tbody>
                    </table>
                </div>
                <div class="cbio-widget-footer"><input type="text" class="cbio-search-input" placeholder="Search..."></div>
                <div class="cbio-resize-handle"></div>
            </div>`;
    }

    return '';
}

// --- Info popover ---
function showInfoPopover(btn) {
    const widget = btn.closest('.cbio-widget');
    if (!widget) return;
    const pop = document.getElementById('cbio-info-popover');
    const attrId = widget.dataset.attrId || '';
    const desc = widget.dataset.description || '';
    const titleText = widget.querySelector('.cbio-widget-title')?.textContent?.trim() || attrId;

    pop.querySelector('.pop-title').textContent = titleText;
    pop.querySelector('.pop-id').textContent = attrId ? `ID: ${attrId}` : '';
    pop.querySelector('.pop-desc').textContent = (desc || '').replace(/ Source:/g, '\nSource:');
    pop.dataset.forWidget = widget.id;
    pop.style.display = 'block';

    const rect = btn.getBoundingClientRect();
    const popW = 260;
    let left = rect.left;
    if (left + popW > window.innerWidth - 8) left = window.innerWidth - popW - 8;
    pop.style.left = left + 'px';
    pop.style.top = (rect.bottom + 4) + 'px';
}

function hideInfoPopover() {
    document.getElementById('cbio-info-popover').style.display = 'none';
}

// --- Toggle view mode (pie <-> table/bar) ---
async function toggleViewMode(attrId) {
    const widget = document.getElementById(`widget-${attrId}`);
    if (!widget) return;
    const chartType = widget.dataset.chartType;
    const viewMode = widget.dataset.viewMode;
    const chartDiv = document.getElementById(`chart-${attrId}`);

    if (chartType === 'table') {
        const tableEl = widget.querySelector('.cbio-table');
        const footerEl = widget.querySelector('.cbio-widget-footer');
        if (viewMode === 'table') {
            // → pie
            if (tableEl) tableEl.style.display = 'none';
            if (footerEl) footerEl.style.display = 'none';
            if (chartDiv) chartDiv.style.display = '';
            widget.dataset.viewMode = 'pie';
            await updatePieWidget(attrId);
        } else {
            // → table
            if (chartDiv) chartDiv.style.display = 'none';
            if (tableEl) tableEl.style.display = '';
            if (footerEl) footerEl.style.display = '';
            // Dispose pie instance so it re-inits fresh on next toggle
            if (Charts.Pies[attrId]) { Charts.Pies[attrId].dispose(); delete Charts.Pies[attrId]; }
            widget.dataset.viewMode = 'table';
            await updateTableWidget(attrId);
        }
    } else if (chartType === 'bar') {
        if (viewMode === 'bar') {
            // Dispose bar → pie
            if (Charts.Bars[attrId]) { Charts.Bars[attrId].dispose(); delete Charts.Bars[attrId]; }
            widget.dataset.viewMode = 'pie';
            await updatePieWidget(attrId);
        } else {
            // Dispose pie → bar
            if (Charts.Pies[attrId]) { Charts.Pies[attrId].dispose(); delete Charts.Pies[attrId]; }
            widget.dataset.viewMode = 'bar';
            await updateBarWidget(attrId);
        }
    }
}

// --- Download helpers ---
function downloadBlob(blob, filename) {
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = filename; a.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
}

function downloadCSV(attrId) {
    const data = widgetData[attrId];
    if (!data || !data.length) return;
    const firstKey = Object.keys(data[0]);
    const lines = [firstKey.join(',')];
    data.forEach(row => {
        lines.push(firstKey.map(k => {
            const v = String(row[k] ?? '');
            return v.includes(',') || v.includes('"') ? `"${v.replace(/"/g, '""')}"` : v;
        }).join(','));
    });
    downloadBlob(new Blob([lines.join('\n')], { type: 'text/csv' }), `${attrId}.csv`);
}

function downloadSVG(attrId) {
    let chart = Charts.Pies[attrId] || Charts.Bars[attrId];
    if (attrId === '_scatter') chart = Charts.Scatter;
    if (attrId === '_km') chart = Charts.KM;
    if (!chart) return;
    const svg = chart.renderToSVGString ? chart.renderToSVGString() : null;
    if (!svg) { alert('SVG export requires SVG renderer.'); return; }
    downloadBlob(new Blob([svg], { type: 'image/svg+xml' }), `${attrId}.svg`);
}

// --- Hamburger menu ---
let _menuAttrId = null;

function showWidgetMenu(btn) {
    const widget = btn.closest('.cbio-widget');
    if (!widget) return;
    const attrId = widget.dataset.attrId;
    const chartType = widget.dataset.chartType;
    const viewMode = widget.dataset.viewMode;
    const menu = document.getElementById('cbio-widget-menu');

    // Toggle off
    if (menu.style.display === 'block' && _menuAttrId === attrId) {
        menu.style.display = 'none';
        _menuAttrId = null;
        return;
    }
    _menuAttrId = attrId;

    const items = [];
    // View toggle
    if (chartType === 'table') {
        const label = viewMode === 'table' ? 'Show as Pie Chart' : 'Show as Table';
        items.push(`<div class="cbio-menu-item" data-action="toggle-view" data-attr-id="${attrId}">${label}</div>`);
        items.push('<div class="cbio-menu-divider"></div>');
    } else if (chartType === 'bar') {
        const label = viewMode === 'bar' ? 'Show as Pie Chart' : 'Show as Bar Chart';
        items.push(`<div class="cbio-menu-item" data-action="toggle-view" data-attr-id="${attrId}">${label}</div>`);
        items.push('<div class="cbio-menu-divider"></div>');
    }
    // Downloads
    items.push('<div class="cbio-menu-section">Download</div>');
    items.push(`<div class="cbio-menu-item" data-action="download-csv" data-attr-id="${attrId}">Summary Data (CSV)</div>`);
    const hasSVGChart = ['pie','bar','_scatter','_km'].includes(chartType) ||
                        (chartType === 'table' && viewMode === 'pie') ||
                        (chartType === 'bar' && viewMode === 'pie');
    if (hasSVGChart) {
        items.push(`<div class="cbio-menu-item" data-action="download-svg" data-attr-id="${attrId}">Image (SVG)</div>`);
    }

    menu.innerHTML = items.join('');
    // Position off-screen first to measure, then place
    menu.style.left = '-9999px';
    menu.style.top = '-9999px';
    menu.style.display = 'block';

    const rect = btn.getBoundingClientRect();
    let left = rect.right - menu.offsetWidth;
    if (left < 4) left = 4;
    menu.style.left = left + 'px';
    menu.style.top = (rect.bottom + 4) + 'px';
}

function hideWidgetMenu() {
    document.getElementById('cbio-widget-menu').style.display = 'none';
    _menuAttrId = null;
}

// Matrix-based bin-packing — direct port of the legacy cBioPortal calculateLayout algorithm.
//
// Key constraint (mirrors legacy isOccupied() in StudyViewUtils.tsx):
//   charts with w > 2 must start at x divisible by 4 (legacy: w>1 → x%2===0, scaled ×2)
//   charts with h > 5 must start at y divisible by 5  (legacy: h>1 → y%2===0, scaled ×5÷2)
// Effect: large charts (w=4 h=10) can start at y=0, 5, 10... allowing them to fill
// alongside the lower half of a KM column instead of leaving a 5-row gap.
function computeLayout(items) {
    const COLS = 12;
    const matrix = [new Array(COLS).fill('')];

    function isOccupied(x, y, w, h) {
        if (w > 2 && x % 4 !== 0) return true;
        if (h > 5 && y % 5 !== 0) return true;
        for (let i = y; i < y + h; i++) {
            if (i >= matrix.length) break;
            for (let j = x; j < x + w; j++) {
                if (j >= COLS || matrix[i][j]) return true;
            }
        }
        return false;
    }

    function findSpot(w, h) {
        for (let y = 0; y < matrix.length; y++) {
            for (let x = 0; x < COLS; x++) {
                if (!matrix[y][x] && !isOccupied(x, y, w, h)) return { x, y };
            }
        }
        return { x: 0, y: matrix.length };
    }

    return items.map(item => {
        const { x, y } = findSpot(item.w, item.h);
        while (y + item.h >= matrix.length) matrix.push(new Array(COLS).fill(''));
        for (let i = y; i < y + item.h; i++)
            for (let j = x; j < x + item.w; j++)
                matrix[i][j] = item.attr_id || 'x';
        return { x, y };
    });
}

function initSearchInputs() {
    const debounceTimers = {};
    document.getElementById('dashboard-grid').addEventListener('input', e => {
        const input = e.target;
        if (!input.classList.contains('cbio-search-input')) return;
        const widget = input.closest('.cbio-widget');
        if (!widget) return;
        const attrId = widget.dataset.attrId;
        if (!attrId) return;
        tableSearchState[attrId] = input.value;
        clearTimeout(debounceTimers[attrId]);
        debounceTimers[attrId] = setTimeout(() => reRenderTableTbody(attrId), 400);
    });
}

function buildDashboard(chartsMeta, grid) {
    const items = chartsMeta.map(c => ({ ...c, html: buildWidgetHTML(c) })).filter(c => c.html);
    const positions = computeLayout(items);
    items.forEach((c, i) => {
        grid.addWidget({ w: c.w, h: c.h, x: positions[i].x, y: positions[i].y, content: c.html });
    });
    updateAll();
    initSearchInputs();
}

// Re-pack remaining widgets into order after a close.
function relayoutDashboard(removedAttrId, grid) {
    DashboardState.chartsMeta = DashboardState.chartsMeta.filter(c => c.attr_id !== removedAttrId);
    const remaining = DashboardState.chartsMeta.filter(c => document.getElementById(`widget-${c.attr_id}`));

    // Sort by current grid position (Y then X) — mirrors legacy calculateLayout() in
    // StudyViewUtils.tsx:2145 which sorts by chartOrderMap[key].y / .x when a layout exists.
    // Charts nearest the top fill the vacated space first.
    remaining.sort((a, b) => {
        const elA = document.getElementById(`widget-${a.attr_id}`)?.closest('.grid-stack-item');
        const elB = document.getElementById(`widget-${b.attr_id}`)?.closest('.grid-stack-item');
        const ay = parseInt(elA?.getAttribute('gs-y') ?? '9999');
        const ax = parseInt(elA?.getAttribute('gs-x') ?? '9999');
        const by = parseInt(elB?.getAttribute('gs-y') ?? '9999');
        const bx = parseInt(elB?.getAttribute('gs-x') ?? '9999');
        return ay !== by ? ay - by : ax - bx;
    });

    const positions = computeLayout(remaining);
    grid.batchUpdate();
    remaining.forEach((c, i) => {
        const gsItem = document.getElementById(`widget-${c.attr_id}`)?.closest('.grid-stack-item');
        if (gsItem) grid.update(gsItem, { x: positions[i].x, y: positions[i].y });
    });
    grid.commit();
    // Keep chartsMeta in the new visual order for subsequent deletions
    DashboardState.chartsMeta = remaining;
}

document.addEventListener('DOMContentLoaded', async function() {
    const grid = GridStack.init({ cellHeight: 30, margin: 5, float: true, draggable: { handle: '.cbio-widget-header' } });

    try {
        const response = await fetch(`/study/summary/charts-meta?id=${encodeURIComponent(DashboardState.studyId)}`);
        DashboardState.chartsMeta = await response.json();
    } catch (e) {
        DashboardState.chartsMeta = [];
    }

    buildDashboard(DashboardState.chartsMeta, grid);

    window.addEventListener('cbio-filter-changed', updateAll);

    const resizer = () => {
        Object.values(Charts.Pies).forEach(c => c.resize());
        Object.values(Charts.Bars).forEach(c => c.resize());
        if (Charts.Scatter) Charts.Scatter.resize();
        if (Charts.KM) Charts.KM.resize();
    };
    grid.on('resizestop', resizer);
    window.addEventListener('resize', resizer);

    // Info button: show on hover, hide on mouse leave
    document.addEventListener('mouseover', e => {
        if (e.target.closest('[data-action="info-widget"]')) {
            showInfoPopover(e.target.closest('[data-action="info-widget"]'));
        }
    });
    document.addEventListener('mouseout', e => {
        const leavingBtn = e.target.closest('[data-action="info-widget"]');
        const leavingPop = e.target.closest('#cbio-info-popover');
        if (leavingBtn || leavingPop) {
            const into = e.relatedTarget;
            if (!into?.closest('[data-action="info-widget"]') && !into?.closest('#cbio-info-popover')) {
                hideInfoPopover();
            }
        }
    });

    // Pie chart hover table popup
    (() => {
        const popup = document.getElementById('cbio-pie-hover-table');
        let hideTimer = null;

        function cancelHide() { if (hideTimer) { clearTimeout(hideTimer); hideTimer = null; } }

        popup.addEventListener('mouseenter', cancelHide);
        popup.addEventListener('mouseleave', () => { hideTimer = setTimeout(() => { popup.style.display = 'none'; }, 150); });

        popup.querySelector('.pht-search').addEventListener('input', function() {
            const q = this.value.toLowerCase();
            popup.querySelectorAll('.pht-row').forEach(row => {
                row.style.display = row.dataset.label.toLowerCase().includes(q) ? '' : 'none';
            });
        });

        popup.querySelector('.pht-select-all-btn').addEventListener('click', () => {
            const attrId = popup.dataset.attrId;
            if (!attrId) return;
            const data = widgetData[attrId] || [];
            const currentFilter = DashboardState.filters.clinicalDataFilters.find(f => f.attributeId === attrId);
            const selectedValues = currentFilter ? currentFilter.values.map(v => v.value) : [];
            if (selectedValues.length === data.length) {
                // Deselect all — remove filter
                DashboardState.filters.clinicalDataFilters = DashboardState.filters.clinicalDataFilters.filter(f => f.attributeId !== attrId);
            } else {
                // Select all
                DashboardState.filters.clinicalDataFilters = DashboardState.filters.clinicalDataFilters.filter(f => f.attributeId !== attrId);
                DashboardState.filters.clinicalDataFilters.push({ attributeId: attrId, values: data.map(d => ({ value: d.value })) });
            }
            document.dispatchEvent(new CustomEvent('cbio-filter-changed'));
        });

        window.showPieHoverTable = function(attrId, hoveredName) {
            cancelHide();
            const data = widgetData[attrId];
            if (!data || data.length === 0) return;
            const widget = document.getElementById(`widget-${attrId}`);
            if (!widget) return;
            const rect = widget.getBoundingClientRect();

            const meta = (DashboardState.chartsMeta || []).find(c => c.attr_id === attrId);
            popup.querySelector('.pht-title').textContent = meta ? meta.display_name : attrId;
            popup.dataset.attrId = attrId;

            const currentFilter = DashboardState.filters.clinicalDataFilters.find(f => f.attributeId === attrId);
            const selectedValues = currentFilter ? currentFilter.values.map(v => v.value) : [];

            popup.querySelector('.pht-rows').innerHTML = data.map(item => {
                const pct = typeof item.pct === 'number' ? item.pct.toFixed(1) + '%' : item.pct;
                const checked = selectedValues.includes(item.value) ? 'checked' : '';
                const hovered = item.value === hoveredName ? ' pht-row-hovered' : '';
                return `<div class="pht-row${hovered}" data-label="${item.value.replace(/"/g,'&quot;')}" data-attr="${attrId}" data-value="${item.value.replace(/"/g,'&quot;')}">
                    <span class="pht-swatch" style="background:${item.color}"></span>
                    <span class="pht-label" title="${item.value}">${item.value}</span>
                    <input type="checkbox" class="pht-checkbox" ${checked}>
                    <span class="pht-count">${item.count.toLocaleString()}</span>
                    <span class="pht-pct">${pct}</span>
                </div>`;
            }).join('');

            // Wire row checkboxes
            popup.querySelectorAll('.pht-row').forEach(row => {
                row.addEventListener('click', (e) => {
                    if (e.target.type === 'checkbox') return; // handled below
                    toggleFilter(row.dataset.attr, row.dataset.value);
                });
                row.querySelector('.pht-checkbox').addEventListener('change', () => {
                    toggleFilter(row.dataset.attr, row.dataset.value);
                });
            });

            // Position: right of widget, fallback to left
            popup.style.display = 'block';
            const pw = popup.offsetWidth;
            const ph = popup.offsetHeight;
            let left = rect.right + 8;
            if (left + pw > window.innerWidth - 8) left = rect.left - pw - 8;
            let top = rect.top;
            if (top + ph > window.innerHeight - 8) top = window.innerHeight - ph - 8;
            popup.style.left = Math.max(8, left) + 'px';
            popup.style.top = Math.max(8, top) + 'px';

            // Reset search
            popup.querySelector('.pht-search').value = '';
            popup.querySelectorAll('.pht-row').forEach(r => r.style.display = '');
        };

        window.schedulePieHoverTableHide = function() {
            hideTimer = setTimeout(() => { popup.style.display = 'none'; }, 150);
        };
    })();

    // Delegated click handler for widget header buttons and menu items
    document.addEventListener('click', e => {
        const btn = e.target.closest('[data-action]');
        if (btn) {
            const action = btn.dataset.action;
            if (action === 'close-widget') {
                e.stopPropagation();
                const item = btn.closest('.grid-stack-item');
                if (!item) return;
                const attrId = item.querySelector('.cbio-widget')?.dataset.attrId;
                grid.removeWidget(item);
                hideInfoPopover(); hideWidgetMenu();
                if (attrId) relayoutDashboard(attrId, grid);
                return;
            }
            if (action === 'menu-widget') {
                e.stopPropagation();
                hideInfoPopover();
                showWidgetMenu(btn);
                return;
            }
            if (action === 'toggle-view') {
                e.stopPropagation();
                hideWidgetMenu();
                toggleViewMode(btn.dataset.attrId);
                return;
            }
            if (action === 'download-csv') {
                e.stopPropagation();
                hideWidgetMenu();
                downloadCSV(btn.dataset.attrId);
                return;
            }
            if (action === 'download-svg') {
                e.stopPropagation();
                hideWidgetMenu();
                downloadSVG(btn.dataset.attrId);
                return;
            }
        }
        // Click outside → close menu
        if (!e.target.closest('#cbio-widget-menu')) hideWidgetMenu();
    });
});
