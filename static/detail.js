function showNote(logId) {
    const noteScript = document.getElementById('note-data-' + logId);
    if (noteScript) {
        try {
            const noteText = noteScript.textContent.trim();
            if (!noteText) {
                throw new Error('空的备注数据');
            }
            const noteData = JSON.parse(noteText);

            // 验证数据结构
            if (!noteData || typeof noteData !== 'object') {
                throw new Error('无效的数据格式');
            }

            const content = renderNoteContent(noteData);
            document.getElementById('noteContent').innerHTML = content;
            document.getElementById('noteModal').style.display = 'flex';
        } catch (e) {
            console.error('Failed to parse note data:', e);
            console.error('Raw note text:', noteScript ? noteScript.textContent : 'No script element');

            // 显示原始数据作为fallback
            const rawText = noteScript ? noteScript.textContent : '无数据';
            document.getElementById('noteContent').innerHTML = `
                <div class="error">
                    <h4>数据解析错误</h4>
                    <p>错误信息: ${e.message}</p>
                    <details>
                        <summary>原始数据</summary>
                        <pre style="background: #f8f9fa; padding: 10px; border-radius: 4px; overflow-x: auto; white-space: pre-wrap;">${rawText}</pre>
                    </details>
                </div>
            `;
            document.getElementById('noteModal').style.display = 'flex';
        }
    } else {
        console.error('No note data found for log ID:', logId);
        document.getElementById('noteContent').innerHTML = '<div class="error">未找到备注数据 (ID: ' + logId + ')</div>';
        document.getElementById('noteModal').style.display = 'flex';
    }
}

function renderNoteContent(noteData) {
    let html = '';

    // 导入时间和总体信息
    html += `<div class="note-section">`;
    html += `<div class="note-time">导入时间: ${noteData.import_time || '未知'}</div>`;
    html += `<div class="note-status ${noteData.has_changes ? 'has-changes' : 'no-changes'}">`;
    html += noteData.has_changes ? '✓ 有变化' : '○ 无变化';
    html += `</div>`;
    html += `</div>`;

    if (noteData.summary && noteData.summary.total_changes > 0) {
        html += `<div class="note-summary">总变化数量: <strong>${noteData.summary.total_changes}</strong></div>`;
    }

    // 户型变化
    if (noteData.floorplans && noteData.floorplans.length > 0) {
        html += `<div class="note-section">`;
        html += `<h4>户型变化</h4>`;
        html += `<div class="changes-list">`;

        noteData.floorplans.forEach(floorplan => {
            const statusClass = getChangeStatusClass(floorplan.change_status);
            const statusText = getChangeStatusText(floorplan.change_status);
            html += `<div class="change-item ${statusClass}">`;
            html += `<span class="change-badge">${statusText}</span>`;
            html += `<span class="item-name">${floorplan.name}</span>`;
            html += `</div>`;
        });

        html += `</div>`;
        html += `</div>`;
    }

    // 房源变化
    if (noteData.properties && noteData.properties.length > 0) {
        html += `<div class="note-section">`;
        html += `<h4>房源变化</h4>`;
        html += `<div class="changes-list">`;

        noteData.properties.forEach(property => {
            const statusClass = getChangeStatusClass(property.change_status);
            const statusText = getChangeStatusText(property.change_status);
            html += `<div class="change-item ${statusClass}">`;
            html += `<div class="change-header">`;
            html += `<span class="change-badge">${statusText}</span>`;
            html += `<span class="item-name">${property.title}</span>`;
            html += `</div>`;
            if (property.change_description) {
                html += `<div class="change-description">${property.change_description}</div>`;
            }
            html += `<div class="property-info">`;
            html += `<span class="property-status status-${property.status.toLowerCase().replace(' ', '-')}">${getPropertyStatusText(property.status)}</span>`;
            if (property.price) {
                html += `<span class="property-price">${property.price.toLocaleString()}</span>`;
            }
            html += `</div>`;
            html += `</div>`;
        });

        html += `</div>`;
        html += `</div>`;
    }

    return html;
}

function getChangeStatusClass(status) {
    const statusMap = {
        'added': 'change-added',
        'removed': 'change-removed',
        'unchanged': 'change-unchanged',
        'status_changed': 'change-modified',
        'price_changed': 'change-modified',
        'status_and_price_changed': 'change-modified'
    };
    return statusMap[status] || 'change-unknown';
}

function getChangeStatusText(status) {
    const statusMap = {
        'added': '新增',
        'removed': '删除',
        'unchanged': '无变化',
        'status_changed': '状态变化',
        'price_changed': '价格变化',
        'status_and_price_changed': '状态价格变化'
    };
    return statusMap[status] || status;
}

function getPropertyStatusText(status) {
    const statusMap = {
        'For Sale': '在售',
        'Coming Soon': '即将推出',
        'Sold': '已售'
    };
    return statusMap[status] || status;
}

function closeModal(event) {
    if (!event || event.target.id === 'noteModal' || event.target.className === 'close') {
        document.getElementById('noteModal').style.display = 'none';
    }
}

// ESC键关闭弹窗
document.addEventListener('keydown', function (event) {
    if (event.key === 'Escape') {
        closeModal();
    }
});

// 选项卡切换功能
function openTab(evt, tabName) {
    var i, tabcontent, tablinks;

    // 隐藏所有选项卡内容
    tabcontent = document.getElementsByClassName("tab-content");
    for (i = 0; i < tabcontent.length; i++) {
        tabcontent[i].classList.remove("active");
    }

    // 移除所有选项卡按钮的active类
    tablinks = document.getElementsByClassName("tab-button");
    for (i = 0; i < tablinks.length; i++) {
        tablinks[i].classList.remove("active");
    }

    // 显示当前选项卡内容并添加active类到按钮
    document.getElementById(tabName).classList.add("active");
    evt.currentTarget.classList.add("active");

    // 如果切换到房源信息选项卡，加载房源数据
    if (tabName === 'properties-tab' && !window.propertiesLoaded) {
        loadProperties();
    }
}

// 加载房源数据
function loadProperties() {
    const communityData = JSON.parse(document.getElementById('community-data').textContent);
    const communityId = communityData.id;
    if (!communityId) return;

    fetch(`/api/properties/${communityId}`)
        .then(response => response.json())
        .then(data => {
            window.allProperties = data;
            window.propertiesLoaded = true;
            renderProperties(data);
        })
        .catch(error => {
            console.error('Error loading properties:', error);
            document.getElementById('properties-container').innerHTML =
                '<p class="error">加载房源数据失败</p>';
        });
}

// 渲染房源列表
function renderProperties(properties) {
    const container = document.getElementById('properties-container');

    if (!properties || properties.length === 0) {
        container.innerHTML = '<p class="hint">暂无房源数据</p>';
        return;
    }

    let html = `
        <div class="properties-table-container">
            <table class="properties-table">
                <thead>
                    <tr>
                        <th>房源标题</th>
                        <th>卧室</th>
                        <th>浴室</th>
                        <th>面积</th>
                        <th>当前价格</th>
                        <th>最低价格</th>
                        <th>最高价格</th>
                        <th>状态</th>
                        <th>创建时间</th>
                        <th>更新时间</th>
                    </tr>
                </thead>
                <tbody>
    `;

    properties.forEach(property => {
        const statusClass = getPropertyStatusClass(property.status);
        const statusText = getPropertyStatusText(property.status);

        html += `
            <tr class="property-row" data-status="${property.status}">
                <td class="property-title">${property.title || '-'}</td>
                <td>${property.bedrooms || '-'}</td>
                <td>${property.bathrooms || '-'}</td>
                <td>${property.size ? property.size.toLocaleString() + ' sq ft' : '-'}</td>
                <td class="price-cell">
                    ${property.price ? '$' + property.price.toLocaleString() : '-'}
                </td>
                <td class="price-cell">
                    ${property.lowest_price ? '$' + property.lowest_price.toLocaleString() : '-'}
                    ${property.lowest_price_time ? '<br><small class="price-time">' + formatDate(property.lowest_price_time) + '</small>' : ''}
                </td>
                <td class="price-cell">
                    ${property.highest_price ? '$' + property.highest_price.toLocaleString() : '-'}
                    ${property.highest_price_time ? '<br><small class="price-time">' + formatDate(property.highest_price_time) + '</small>' : ''}
                </td>
                <td>
                    <span class="property-status ${statusClass}">${statusText}</span>
                </td>
                <td class="time-cell">${formatDate(property.created_time)}</td>
                <td class="time-cell">${formatDate(property.update_time)}</td>
            </tr>
        `;
    });

    html += `
                </tbody>
            </table>
        </div>
    `;

    container.innerHTML = html;
}

// 筛选房源
function filterProperties() {
    if (!window.allProperties) return;

    const statusFilter = document.getElementById('statusFilter').value;
    const searchText = document.getElementById('searchInput').value.toLowerCase();

    let filteredProperties = window.allProperties;

    // 按状态筛选
    if (statusFilter) {
        filteredProperties = filteredProperties.filter(p => p.status === statusFilter);
    }

    // 按搜索文本筛选
    if (searchText) {
        filteredProperties = filteredProperties.filter(p =>
            (p.title && p.title.toLowerCase().includes(searchText)) ||
            (p.bedrooms && p.bedrooms.toString().includes(searchText)) ||
            (p.bathrooms && p.bathrooms.toString().includes(searchText)) ||
            (p.size && p.size.toString().includes(searchText)) ||
            (p.price && p.price.toString().includes(searchText))
        );
    }

    renderProperties(filteredProperties);
}

// 获取房源状态样式类
function getPropertyStatusClass(status) {
    const statusMap = {
        'For Sale': 'status-sale',
        'Coming Soon': 'status-coming',
        'Sold': 'status-sold'
    };
    return statusMap[status] || 'status-unknown';
}

// 格式化日期
function formatDate(dateString) {
    if (!dateString) return '-';
    const date = new Date(dateString);
    return date.toLocaleDateString('zh-CN') + ' ' + date.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
}
