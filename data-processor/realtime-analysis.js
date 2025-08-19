const fs = require('fs').promises;
const csv = require('csv-parser');
const createReadStream = require('fs').createReadStream;

/**
 * 日時文字列をDateオブジェクトに変換
 * @param {string} dateStr - "2024-03-29 23:33:42 +0900" 形式
 * @returns {Date}
 */
function parseDateTime(dateStr) {
    return new Date(dateStr);
}

/**
 * 滞在時間文字列を分に変換
 * @param {string} timeStr - "01:23:45" 形式
 * @returns {number} 分数
 */
function parseStayTime(timeStr) {
    const [hours, minutes, seconds] = timeStr.split(':').map(Number);
    return hours * 60 + minutes + seconds / 60;
}

/**
 * 時間スロットを生成（チェックイン〜チェックアウトの各時間）
 * @param {Date} checkin - チェックイン時刻
 * @param {Date} checkout - チェックアウト時刻
 * @returns {Array} 時間スロットの配列
 */
function generateTimeSlots(checkin, checkout) {
    const slots = [];
    const current = new Date(checkin);
    
    // 1時間ごとにスロットを生成
    while (current < checkout) {
        const slotEnd = new Date(current);
        slotEnd.setHours(slotEnd.getHours() + 1, 0, 0, 0); // 次の時間の00分00秒
        
        const actualEnd = slotEnd > checkout ? checkout : slotEnd;
        const duration = (actualEnd - current) / (1000 * 60); // 分単位
        
        if (duration > 0) {
            slots.push({
                date: current.toISOString().split('T')[0], // YYYY-MM-DD
                hour: current.getHours(),
                dateHour: `${current.toISOString().split('T')[0]}-${current.getHours().toString().padStart(2, '0')}`,
                duration: duration,
                checkin: current.toISOString(),
                checkout: actualEnd.toISOString()
            });
        }
        
        current.setTime(slotEnd.getTime());
    }
    
    return slots;
}

/**
 * 時間別在館者数を計算
 * @param {Array} records - CSVレコード
 * @returns {Object} 時間別在館者数データ
 */
function calculateHourlyOccupancy(records) {
    const hourlyOccupancy = {};
    const dailyStats = {};
    const allTimeSlots = [];
    
    console.log(`📊 ${records.length}件のレコードから時間別在館者数を計算中...`);
    
    records.forEach((record, index) => {
        try {
            const checkin = parseDateTime(record['チェックイン日時']);
            const checkout = record['チェックアウト日時'] ? 
                parseDateTime(record['チェックアウト日時']) : null;
            const stayMinutes = parseStayTime(record['滞在時間']);
            
            // チェックアウト時刻がない場合は滞在時間から計算
            if (!checkout && stayMinutes > 0) {
                checkout = new Date(checkin.getTime() + stayMinutes * 60 * 1000);
            }
            
            if (!checkout || stayMinutes <= 0) {
                console.warn(`⚠️  スキップ: 不正な時間データ (行${index + 2})`);
                return;
            }
            
            // 時間スロットを生成
            const timeSlots = generateTimeSlots(checkin, checkout);
            allTimeSlots.push(...timeSlots.map(slot => ({
                ...slot,
                customerName: record['顧客名'],
                originalStayMinutes: stayMinutes
            })));
            
            // 時間別在館者数をカウント
            timeSlots.forEach(slot => {
                if (!hourlyOccupancy[slot.dateHour]) {
                    hourlyOccupancy[slot.dateHour] = {
                        count: 0,
                        totalMinutes: 0,
                        users: []
                    };
                }
                hourlyOccupancy[slot.dateHour].count++;
                hourlyOccupancy[slot.dateHour].totalMinutes += slot.duration;
                hourlyOccupancy[slot.dateHour].users.push({
                    name: record['顧客名'],
                    duration: slot.duration
                });
            });
            
            // 日別統計
            const date = checkin.toISOString().split('T')[0];
            if (!dailyStats[date]) {
                dailyStats[date] = {
                    totalHours: 0,
                    totalSessions: 0,
                    uniqueUsers: new Set()
                };
            }
            dailyStats[date].totalHours += stayMinutes / 60;
            dailyStats[date].totalSessions++;
            dailyStats[date].uniqueUsers.add(record['顧客名']);
            
        } catch (error) {
            console.warn(`⚠️  データ解析エラー (行${index + 2}):`, error.message);
        }
    });
    
    // uniqueUsersをSetから数値に変換
    Object.keys(dailyStats).forEach(date => {
        dailyStats[date].uniqueUsers = dailyStats[date].uniqueUsers.size;
    });
    
    console.log(`✅ 時間別データ生成完了: ${Object.keys(hourlyOccupancy).length}時間スロット`);
    
    return {
        hourlyOccupancy,
        dailyStats,
        allTimeSlots
    };
}

/**
 * 同期間比較データを生成
 * @param {Array} records - 全CSVレコード
 * @returns {Object} 比較分析データ
 */
function generateComparisonData(records) {
    const currentDate = new Date();
    const currentMonth = currentDate.getMonth() + 1; // 1-12
    const currentYear = currentDate.getFullYear();
    const currentDay = currentDate.getDate();
    
    // 当月データ (1日〜現在日まで)
    const currentMonthStart = new Date(currentYear, currentMonth - 1, 1);
    const currentMonthEnd = new Date(currentYear, currentMonth - 1, currentDay, 23, 59, 59);
    
    // 前月同期間データ (1日〜同じ日数まで)
    const previousMonth = currentMonth === 1 ? 12 : currentMonth - 1;
    const previousYear = currentMonth === 1 ? currentYear - 1 : currentYear;
    const previousMonthStart = new Date(previousYear, previousMonth - 1, 1);
    const previousMonthEnd = new Date(previousYear, previousMonth - 1, currentDay, 23, 59, 59);
    
    console.log(`📅 比較期間:`);
    console.log(`   当月: ${currentMonthStart.toISOString().split('T')[0]} 〜 ${currentMonthEnd.toISOString().split('T')[0]}`);
    console.log(`   前月: ${previousMonthStart.toISOString().split('T')[0]} 〜 ${previousMonthEnd.toISOString().split('T')[0]}`);
    
    // データフィルタリング
    const currentMonthRecords = records.filter(record => {
        const checkin = parseDateTime(record['チェックイン日時']);
        return checkin >= currentMonthStart && checkin <= currentMonthEnd;
    });
    
    const previousMonthRecords = records.filter(record => {
        const checkin = parseDateTime(record['チェックイン日時']);
        return checkin >= previousMonthStart && checkin <= previousMonthEnd;
    });
    
    console.log(`📊 データ件数: 当月${currentMonthRecords.length}件, 前月${previousMonthRecords.length}件`);
    
    // 各月の時間別データを生成
    const currentMonthData = calculateHourlyOccupancy(currentMonthRecords);
    const previousMonthData = calculateHourlyOccupancy(previousMonthRecords);
    
    // 総合統計を計算
    const currentTotalStats = calculateTotalStats(currentMonthData, currentMonthRecords);
    const previousTotalStats = calculateTotalStats(previousMonthData, previousMonthRecords);
    
    // 変化率を計算
    const comparison = calculateComparison(currentTotalStats, previousTotalStats);
    
    return {
        metadata: {
            generatedAt: new Date().toISOString(),
            currentPeriod: `${currentYear}-${currentMonth.toString().padStart(2, '0')}-01 to ${currentYear}-${currentMonth.toString().padStart(2, '0')}-${currentDay.toString().padStart(2, '0')}`,
            previousPeriod: `${previousYear}-${previousMonth.toString().padStart(2, '0')}-01 to ${previousYear}-${previousMonth.toString().padStart(2, '0')}-${currentDay.toString().padStart(2, '0')}`,
            comparisonDays: currentDay
        },
        currentMonth: {
            period: `${currentYear}-${currentMonth.toString().padStart(2, '0')}`,
            records: currentMonthRecords.length,
            ...currentMonthData,
            totalStats: currentTotalStats
        },
        previousMonth: {
            period: `${previousYear}-${previousMonth.toString().padStart(2, '0')}`,
            records: previousMonthRecords.length,
            ...previousMonthData,
            totalStats: previousTotalStats
        },
        comparison
    };
}

/**
 * 総合統計を計算
 * @param {Object} monthData - 月別データ
 * @param {Array} records - レコード
 * @returns {Object} 総合統計
 */
function calculateTotalStats(monthData, records) {
    const { hourlyOccupancy, dailyStats, allTimeSlots } = monthData;
    
    // 総利用時間
    const totalHours = Object.values(dailyStats).reduce((sum, day) => sum + day.totalHours, 0);
    
    // 延べ利用人数 (総利用セッション数)
    // 1回の利用 = 1人として計算（より直感的）
    const manHours = records.length;
    
    // ユニークユーザー数
    const uniqueUsers = new Set(records.map(r => r['顧客名'])).size;
    
    // 総セッション数
    const totalSessions = records.length;
    
    // ピーク時在館者数
    const peakOccupancy = Math.max(...Object.values(hourlyOccupancy).map(h => h.count), 0);
    
    // 平均在館者数 (全時間スロットの平均)
    const averageOccupancy = Object.keys(hourlyOccupancy).length > 0 ?
        Object.values(hourlyOccupancy).reduce((sum, h) => sum + h.count, 0) / Object.keys(hourlyOccupancy).length : 0;
    
    return {
        totalHours: Math.round(totalHours * 10) / 10,
        manHours: Math.round(manHours * 10) / 10,
        uniqueUsers,
        totalSessions,
        peakOccupancy,
        averageOccupancy: Math.round(averageOccupancy * 10) / 10,
        activeDays: Object.keys(dailyStats).length
    };
}

/**
 * 変化率を計算
 * @param {Object} current - 当月統計
 * @param {Object} previous - 前月統計
 * @returns {Object} 比較結果
 */
function calculateComparison(current, previous) {
    const calculateChange = (curr, prev) => {
        if (prev === 0) return curr > 0 ? '+∞%' : '0%';
        const change = ((curr - prev) / prev) * 100;
        return `${change >= 0 ? '+' : ''}${Math.round(change * 10) / 10}%`;
    };
    
    const calculateDifference = (curr, prev) => {
        const diff = curr - prev;
        return `${diff >= 0 ? '+' : ''}${diff}`;
    };
    
    return {
        totalHoursChange: calculateChange(current.totalHours, previous.totalHours),
        manHoursChange: calculateChange(current.manHours, previous.manHours),
        uniqueUsersChange: calculateDifference(current.uniqueUsers, previous.uniqueUsers),
        totalSessionsChange: calculateDifference(current.totalSessions, previous.totalSessions),
        peakOccupancyChange: calculateDifference(current.peakOccupancy, previous.peakOccupancy),
        averageOccupancyChange: calculateChange(current.averageOccupancy, previous.averageOccupancy)
    };
}

/**
 * CSVデータを読み込み
 * @param {string} csvPath - CSVファイルパス
 * @returns {Promise<Array>} レコード配列
 */
async function loadCSVData(csvPath = 'nihouse.csv') {
    const records = [];
    
    return new Promise((resolve, reject) => {
        createReadStream(csvPath)
            .pipe(csv())
            .on('data', (row) => {
                // 必要なフィールドが存在するかチェック
                if (row['顧客名'] && row['チェックイン日時'] && row['滞在時間']) {
                    records.push(row);
                }
            })
            .on('end', () => {
                console.log(`✅ CSVデータ読み込み完了: ${records.length}件`);
                resolve(records);
            })
            .on('error', reject);
    });
}

/**
 * メイン処理
 */
async function main() {
    try {
        console.log('🚀 リアルタイム比較分析を開始...');
        
        // CSVデータ読み込み
        const records = await loadCSVData();
        
        // 比較データ生成
        const analysisData = generateComparisonData(records);
        
        // 結果をJSONファイルに保存
        await fs.writeFile('../docs/realtime-analysis.json', JSON.stringify(analysisData, null, 2));
        console.log('📄 メインデータ保存完了: ../docs/realtime-analysis.json');
        
        // 中間データも保存
        await saveIntermediateData(analysisData);
        
        // サマリー表示
        displaySummary(analysisData);
        
        console.log('\n✅ リアルタイム比較分析完了!');
        
    } catch (error) {
        console.error('❌ エラーが発生しました:', error);
        process.exit(1);
    }
}

/**
 * 中間データを保存
 * @param {Object} analysisData - 分析データ
 */
async function saveIntermediateData(analysisData) {
    try {
        // 時間別在館者数データ (CSV形式)
        const currentHourlyCSV = generateHourlyCSV(analysisData.currentMonth.hourlyOccupancy, 'current');
        const previousHourlyCSV = generateHourlyCSV(analysisData.previousMonth.hourlyOccupancy, 'previous');
        
        await fs.writeFile('../docs/current-month-hourly.csv', currentHourlyCSV);
        await fs.writeFile('../docs/previous-month-hourly.csv', previousHourlyCSV);
        
        // 日別統計データ (JSON形式)
        await fs.writeFile('../docs/daily-stats.json', JSON.stringify({
            current: analysisData.currentMonth.dailyStats,
            previous: analysisData.previousMonth.dailyStats,
            metadata: analysisData.metadata
        }, null, 2));
        
        // タイムスロット詳細データ (JSON形式)
        await fs.writeFile('../docs/time-slots-detail.json', JSON.stringify({
            current: analysisData.currentMonth.allTimeSlots,
            previous: analysisData.previousMonth.allTimeSlots,
            metadata: analysisData.metadata
        }, null, 2));
        
        console.log('📁 中間データ保存完了:');
        console.log('   - current-month-hourly.csv (当月時間別)');
        console.log('   - previous-month-hourly.csv (前月時間別)');
        console.log('   - daily-stats.json (日別統計)');
        console.log('   - time-slots-detail.json (タイムスロット詳細)');
        
    } catch (error) {
        console.error('❌ 中間データ保存エラー:', error);
    }
}

/**
 * 時間別データをCSV形式に変換
 * @param {Object} hourlyData - 時間別データ
 * @param {string} prefix - ファイル接頭辞
 * @returns {string} CSV文字列
 */
function generateHourlyCSV(hourlyData, prefix) {
    const headers = ['日時', '在館者数', '総利用時間(分)', 'ユーザー詳細'];
    const rows = [headers.join(',')];
    
    Object.entries(hourlyData)
        .sort(([a], [b]) => a.localeCompare(b))
        .forEach(([dateHour, data]) => {
            const userDetail = data.users.map(u => `${u.name}(${Math.round(u.duration)}分)`).join(';');
            rows.push([
                dateHour,
                data.count,
                Math.round(data.totalMinutes),
                `"${userDetail}"`
            ].join(','));
        });
    
    return rows.join('\n');
}

/**
 * サマリーを表示
 * @param {Object} analysisData - 分析データ
 */
function displaySummary(analysisData) {
    const { currentMonth, previousMonth, comparison, metadata } = analysisData;
    
    console.log('\n📊 ===== リアルタイム比較分析結果 =====');
    console.log(`📅 比較期間: ${metadata.comparisonDays}日間`);
    console.log(`   当月: ${metadata.currentPeriod}`);
    console.log(`   前月: ${metadata.previousPeriod}`);
    
    console.log('\n📈 総合指標比較:');
    console.log(`   延べ利用人数: ${currentMonth.totalStats.manHours} vs ${previousMonth.totalStats.manHours} (${comparison.manHoursChange})`);
    console.log(`   総利用時間: ${currentMonth.totalStats.totalHours}h vs ${previousMonth.totalStats.totalHours}h (${comparison.totalHoursChange})`);
    console.log(`   ピーク在館者数: ${currentMonth.totalStats.peakOccupancy}人 vs ${previousMonth.totalStats.peakOccupancy}人 (${comparison.peakOccupancyChange}人)`);
    console.log(`   平均在館者数: ${currentMonth.totalStats.averageOccupancy}人 vs ${previousMonth.totalStats.averageOccupancy}人 (${comparison.averageOccupancyChange})`);
    console.log(`   ユニークユーザー: ${currentMonth.totalStats.uniqueUsers}人 vs ${previousMonth.totalStats.uniqueUsers}人 (${comparison.uniqueUsersChange}人)`);
}

// スクリプト実行
if (require.main === module) {
    main();
}

module.exports = {
    generateComparisonData,
    calculateHourlyOccupancy,
    parseDateTime,
    parseStayTime,
    generateTimeSlots
};
