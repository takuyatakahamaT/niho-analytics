const fs = require('fs').promises;
const path = require('path');
const csv = require('csv-parser');

// 日時文字列をDateオブジェクトに変換
function parseDateTime(dateStr) {
    try {
        if (!dateStr) return null;
        
        // "2024-03-29 23:33:42 +0900" 形式に対応
        // タイムゾーン情報を除去して処理
        const cleanDateStr = dateStr.replace(/\s*\+\d{4}$/, '');
        
        // ISO 8601形式に変換を試す
        if (cleanDateStr.includes('-')) {
            // "2024-03-29 23:33:42" 形式
            return new Date(cleanDateStr);
        } else {
            // "2025/8/1 9:15:50" 形式（従来対応）
            const [datePart, timePart] = cleanDateStr.split(' ');
            const [year, month, day] = datePart.split('/').map(Number);
            
            if (!timePart) {
                return new Date(year, month - 1, day);
            }
            
            const [hour, minute, second] = timePart.split(':').map(Number);
            return new Date(year, month - 1, day, hour, minute, second);
        }
    } catch (error) {
        console.error('日時解析エラー:', dateStr, error);
        return null;
    }
}

// 滞在時間を分に変換
function parseStayTime(timeStr) {
    try {
        if (!timeStr || timeStr === '0' || timeStr === '') return 0;
        
        const parts = timeStr.split(':');
        if (parts.length === 3) {
            const [hours, minutes, seconds] = parts.map(Number);
            return hours * 60 + minutes + seconds / 60;
        } else if (parts.length === 2) {
            const [hours, minutes] = parts.map(Number);
            return hours * 60 + minutes;
        } else {
            return parseFloat(timeStr) || 0;
        }
    } catch (error) {
        console.error('滞在時間解析エラー:', timeStr, error);
        return 0;
    }
}

// 年月文字列を取得
function getYearMonth(date) {
    if (!date) return null;
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    return `${year}-${month}`;
}

// 時間帯を判定
function getTimeSlot(hour) {
    if (hour >= 8 && hour < 12) return 'morning';   // 朝（8-12時）
    if (hour >= 12 && hour < 18) return 'afternoon'; // 昼（12-18時）
    if (hour >= 18 && hour < 23) return 'evening';   // 夜（18-23時）
    return 'other'; // その他の時間帯
}

// 曜日を取得（0:日曜日 〜 6:土曜日）
function getDayOfWeek(date) {
    return date.getDay();
}

// 曜日名を取得
function getDayName(dayIndex) {
    const dayNames = ['日曜日', '月曜日', '火曜日', '水曜日', '木曜日', '金曜日', '土曜日'];
    return dayNames[dayIndex];
}

// CSVファイルを読み込む
async function loadCSV(filePath) {
    const records = [];
    
    return new Promise((resolve, reject) => {
        const stream = require('fs').createReadStream(filePath)
            .pipe(csv())
            .on('data', (row) => {
                // データをクリーニング
                const cleanedRow = {};
                Object.keys(row).forEach(key => {
                    const cleanKey = key.trim();
                    cleanedRow[cleanKey] = row[key] ? row[key].trim() : '';
                });
                
                const checkinDate = parseDateTime(cleanedRow['チェックイン日時']);
                const stayTime = parseStayTime(cleanedRow['滞在時間']);
                
                if (checkinDate && stayTime > 0) {
                    records.push({
                        checkinDate,
                        stayTime,
                        userId: cleanedRow['会員番号'] || cleanedRow['ユーザーID'] || cleanedRow['メンバーID'] || '',
                        yearMonth: getYearMonth(checkinDate),
                        hour: checkinDate.getHours(),
                        dayOfWeek: getDayOfWeek(checkinDate),
                        timeSlot: getTimeSlot(checkinDate.getHours())
                    });
                }
            })
            .on('end', () => {
                console.log(`CSVファイル読み込み完了: ${records.length}件のレコード`);
                resolve(records);
            })
            .on('error', reject);
    });
}

// 時間帯別分析を実行
function analyzeTimeSlots(records, targetMonth) {
    const monthRecords = records.filter(r => r.yearMonth === targetMonth);
    
    // 時間帯別の統計
    const timeSlotStats = {
        morning: { sessions: [], users: new Set(), totalHours: 0 },
        afternoon: { sessions: [], users: new Set(), totalHours: 0 },
        evening: { sessions: [], users: new Set(), totalHours: 0 }
    };
    
    // 日別の時間帯別在館者数を記録
    const dailyOccupancy = {};
    
    monthRecords.forEach(record => {
        const { timeSlot, userId, stayTime, checkinDate } = record;
        
        if (timeSlot !== 'other') {
            timeSlotStats[timeSlot].sessions.push(record);
            timeSlotStats[timeSlot].users.add(userId);
            timeSlotStats[timeSlot].totalHours += stayTime / 60;
            
            // 日別記録
            const dateKey = checkinDate.toISOString().split('T')[0];
            if (!dailyOccupancy[dateKey]) {
                dailyOccupancy[dateKey] = { morning: 0, afternoon: 0, evening: 0 };
            }
            dailyOccupancy[dateKey][timeSlot]++;
        }
    });
    
    // 平均値を計算
    const daysInMonth = Object.keys(dailyOccupancy).length;
    const averages = {};
    
    Object.keys(timeSlotStats).forEach(slot => {
        const stats = timeSlotStats[slot];
        const dailyTotals = Object.values(dailyOccupancy).map(day => day[slot] || 0);
        const averageOccupancy = daysInMonth > 0 ? dailyTotals.reduce((sum, val) => sum + val, 0) / daysInMonth : 0;
        
        averages[slot] = {
            averageOccupancy: Math.round(averageOccupancy * 10) / 10,
            totalSessions: stats.sessions.length,
            uniqueUsers: stats.users.size,
            totalHours: Math.round(stats.totalHours * 10) / 10,
            averageSessionLength: stats.sessions.length > 0 ? Math.round((stats.totalHours / stats.sessions.length) * 10) / 10 : 0
        };
    });
    
    return {
        timeSlotAverages: averages,
        dailyOccupancy,
        daysAnalyzed: daysInMonth,
        totalRecords: monthRecords.length
    };
}

// 曜日別分析を実行
function analyzeDayOfWeek(records, targetMonth) {
    const monthRecords = records.filter(r => r.yearMonth === targetMonth);
    
    // 曜日別の統計（0:日曜日 〜 6:土曜日）
    const dayStats = {};
    for (let i = 0; i < 7; i++) {
        dayStats[i] = { sessions: [], users: new Set(), totalHours: 0, dates: new Set() };
    }
    
    monthRecords.forEach(record => {
        const { dayOfWeek, userId, stayTime, checkinDate } = record;
        const dateKey = checkinDate.toISOString().split('T')[0];
        
        dayStats[dayOfWeek].sessions.push(record);
        dayStats[dayOfWeek].users.add(userId);
        dayStats[dayOfWeek].totalHours += stayTime / 60;
        dayStats[dayOfWeek].dates.add(dateKey);
    });
    
    // 平均値を計算
    const averages = {};
    Object.keys(dayStats).forEach(day => {
        const stats = dayStats[day];
        const daysCount = stats.dates.size; // その曜日が何日あったか
        
        averages[day] = {
            dayName: getDayName(parseInt(day)),
            averageOccupancy: daysCount > 0 ? Math.round((stats.sessions.length / daysCount) * 10) / 10 : 0,
            totalSessions: stats.sessions.length,
            uniqueUsers: stats.users.size,
            totalHours: Math.round(stats.totalHours * 10) / 10,
            daysAnalyzed: daysCount,
            averageSessionLength: stats.sessions.length > 0 ? Math.round((stats.totalHours / stats.sessions.length) * 10) / 10 : 0
        };
    });
    
    return averages;
}

// 複数月の比較分析を実行
function compareMultipleMonths(records, months) {
    const comparison = {};
    
    months.forEach(month => {
        console.log(`${month}の分析を開始...`);
        
        const timeSlotAnalysis = analyzeTimeSlots(records, month);
        const dayOfWeekAnalysis = analyzeDayOfWeek(records, month);
        
        comparison[month] = {
            timeSlots: timeSlotAnalysis,
            dayOfWeek: dayOfWeekAnalysis,
            metadata: {
                month: month,
                totalRecords: records.filter(r => r.yearMonth === month).length
            }
        };
        
        console.log(`${month}の分析完了: ${comparison[month].metadata.totalRecords}件のレコード`);
    });
    
    return comparison;
}

// メイン処理
async function main() {
    try {
        console.log('時間帯別・曜日別分析を開始...');
        
        // CSVファイルを読み込み
        const csvPath = path.join(__dirname, 'niho-use-08.csv');
        const records = await loadCSV(csvPath);
        
        if (records.length === 0) {
            console.error('有効なレコードが見つかりません');
            return;
        }
        
        // 利用可能な月を取得
        const availableMonths = [...new Set(records.map(r => r.yearMonth))].sort();
        console.log('利用可能な月:', availableMonths);
        
        // 最新の3ヶ月を分析対象とする
        const targetMonths = availableMonths.slice(-3);
        console.log('分析対象月:', targetMonths);
        
        // 複数月比較分析を実行
        const comparison = compareMultipleMonths(records, targetMonths);
        
        // 結果をJSONファイルに保存
        const outputPath = path.join(__dirname, '../docs/time-analysis.json');
        await fs.writeFile(outputPath, JSON.stringify({
            comparison,
            metadata: {
                generatedAt: new Date().toISOString(),
                totalRecords: records.length,
                analysisMonths: targetMonths,
                timeSlots: {
                    morning: '朝（8-12時）',
                    afternoon: '昼（12-18時）',
                    evening: '夜（18-23時）'
                }
            }
        }, null, 2));
        
        console.log('時間帯別・曜日別分析完了');
        console.log(`結果を保存: ${outputPath}`);
        
        // 結果の概要を表示
        targetMonths.forEach(month => {
            console.log(`\n=== ${month} ===`);
            const data = comparison[month];
            
            console.log('時間帯別平均利用人数:');
            Object.entries(data.timeSlots.timeSlotAverages).forEach(([slot, stats]) => {
                const slotName = slot === 'morning' ? '朝' : slot === 'afternoon' ? '昼' : '夜';
                console.log(`  ${slotName}: ${stats.averageOccupancy}人/日`);
            });
            
            console.log('曜日別平均利用人数:');
            Object.entries(data.dayOfWeek).forEach(([day, stats]) => {
                console.log(`  ${stats.dayName}: ${stats.averageOccupancy}人/日`);
            });
        });
        
    } catch (error) {
        console.error('分析中にエラーが発生しました:', error);
    }
}

// スクリプトが直接実行された場合のみmain関数を呼び出し
if (require.main === module) {
    main();
}

module.exports = {
    analyzeTimeSlots,
    analyzeDayOfWeek,
    compareMultipleMonths,
    loadCSV
};
