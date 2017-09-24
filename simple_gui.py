import sys
import pathlib
import os
import pandas as pd
from datetime import datetime
from PyQt5.QtCore import QSettings, QPoint, QSize
from PyQt5.QtWidgets import (QMainWindow, QAction, QMenu, qApp,
            QApplication, QWidget, QDesktopWidget, QPushButton, QGridLayout, QTableView,
            QHBoxLayout, QVBoxLayout, QFileDialog, QListWidget, QListWidgetItem, QLineEdit,
            QLabel)
from PyQt5.QtGui import QIcon
from AssetUtils import AssetDatabase, AssetPortfolio
from PandasModel import PandasModel

class App(QMainWindow):

    data_folder = 'data/'

    def __init__(self):
        super().__init__()
        self.portfolio = AssetPortfolio()
        # portfolio.asset_db.load_asset_historicals('data/my_asset_prices')
        self.initUI()

    def initUI(self):

        self.createActions()
        self.createMenus()
        self.createStatusBar()

        self.createCentralWidget()

        self.setWindowTitle('AssetUtils')
        self.setWindowIcon(QIcon('icon2.png'))
        self.readSettings()
        self.show()

    def closeEvent(self, event):
        self.writeSettings()
        event.accept()

    def readSettings(self):
        settings = QSettings("avlasov", "demo_app")
        pos = settings.value("pos", QPoint(200, 200))
        size = settings.value("size", QSize(400, 400))
        self.resize(size)
        self.move(pos)

    def writeSettings(self):
        settings = QSettings("avlasov", "demo_app")
        settings.setValue("pos", self.pos())
        settings.setValue("size", self.size())

    def createActions(self):
        self.exitAct = QAction('&Exit', self, shortcut = 'Ctrl+Q', triggered = qApp.quit)

    def createMenus(self):
        self.fileMenu = self.menuBar().addMenu('&File')
        self.fileMenu.addAction(self.exitAct)

    def createStatusBar(self):
        self.statusBar()

    def createCentralWidget(self):
        hbox = QHBoxLayout()

        db_widget = self.createDatabaseWidget()
        db_table = self.createDatabaseTable()

        hbox.addItem(db_table)
        hbox.addItem(db_widget)

        centralwidget = QWidget()
        centralwidget.setLayout(hbox)
        self.setCentralWidget(centralwidget)

    def createDatabaseTable(self):
        layout = QVBoxLayout()
        search_box = QHBoxLayout()

        self.tableAssetDbSearchForm = QLineEdit()
        self.tableAssetDbSearchButton = QPushButton('Search', clicked = self.buttonSearchDbClicked)
        self.tableAssetDbLabel = QLabel('Available assets:')
        self.tableAssetDb = QTableView(doubleClicked = self.tableAssetDbClicked)
        self.tableAssetDb.setSortingEnabled(True)
        self.tableAssetDb.setSelectionBehavior(QTableView.SelectRows)
        self.tableAssetDb.verticalHeader().setVisible(False)

        self.tablePortfolioAssetsLabel = QLabel('Assets in portfolio:')
        self.tablePortfolioAssets = QTableView(doubleClicked = self.tablePortfolioAssetsTableClicked)
        self.tablePortfolioAssets.setSortingEnabled(True)
        self.tablePortfolioAssets.setSelectionBehavior(QTableView.SelectRows)
        self.tablePortfolioAssets.verticalHeader().setVisible(False)


        search_box.addWidget(self.tableAssetDbSearchForm)
        search_box.addWidget(self.tableAssetDbSearchButton)
        layout.addWidget(self.tableAssetDbLabel)
        layout.addItem(search_box)
        layout.addWidget(self.tableAssetDb)
        layout.addWidget(self.tablePortfolioAssetsLabel)
        layout.addWidget(self.tablePortfolioAssets)

        self.updateDatabaseTable()
        self.updatePortfolioAssetsTable()
        return layout

    def createDatabaseWidget(self):
        layout = QVBoxLayout()
        self.listAssetDb = QListWidget()
        self.listAssetDb.itemSelectionChanged.connect(self.listAssetDbChanged)
        self.listAssetDb.itemDoubleClicked.connect(self.buttonLoadDbClicked)

        self.listAssetDbRefresh()

        self.buttonLoadDb = QPushButton('Load asset database', clicked = self.buttonLoadDbClicked)
        self.buttonRemoveDb = QPushButton('Remove asset database', clicked = self.buttonRemoveDbClicked)
        self.buttonRetrDb = QPushButton('Retrieve asset database', clicked = self.buttonRetrDbClicked)
        self.buttonLoadDb.setEnabled(False)
        self.buttonRemoveDb.setEnabled(False)
        layout.addWidget(self.listAssetDb)
        layout.addWidget(self.buttonLoadDb)
        layout.addWidget(self.buttonRemoveDb)
        layout.addWidget(self.buttonRetrDb)
        return layout

    def buttonSearchDbClicked(self):
        token = self.tableAssetDbSearchForm.text()
        result = self.portfolio.asset_db.find_in_database(token)
        self.updateDatabaseTable(result)

    def updateDatabaseTable(self, data = None):
        if data is None:
            self.updateDatabaseTable(self.portfolio.asset_db._db)
        else:
            # columns_map = { 'name': 'Name',
            #                 'exchange': 'Exchange',
            #                 'type': 'Type',
            #                 'ticker': 'Ticker'}
            model = PandasModel(data.drop(['href'], axis = 1))
            self.tableAssetDb.setModel(model)

    def updatePortfolioAssetsTable(self):
        assets = pd.DataFrame(columns = self.portfolio.asset_db.db_columns)
        for asset in self.portfolio.asset_list:
            assets = assets.append(self.portfolio.asset_db.get_entry_from_database(asset))
        model = PandasModel(assets.drop(['href'], axis = 1))
        self.tablePortfolioAssets.setModel(model)

    def tableAssetDbClicked(self, index):
        row = index.row()
        name = index.sibling(row, 0).data()
        ticker = index.sibling(row, 2).data()
        asset_type = index.sibling(row, 3).data()
        if asset_type == 'etf':
            self.portfolio.add_asset(ticker)
        elif asset_type == 'pif':
            self.portfolio.add_asset(name)
        else:
            raise ValueError
        self.updatePortfolioAssetsTable()

    def tablePortfolioAssetsTableClicked(self, index):
        row = index.row()
        name = index.sibling(row, 0).data()
        ticker = index.sibling(row, 2).data()
        asset_type = index.sibling(row, 3).data()
        if asset_type == 'etf':
            self.portfolio.remove_asset(ticker)
        elif asset_type == 'pif':
            self.portfolio.remove_asset(name)
        else:
            raise ValueError
        self.updatePortfolioAssetsTable()

    ### Asset Database Manager

    def listAssetDbRefresh(self):
        self.listAssetDb.clear()
        path = pathlib.Path(self.data_folder)
        for fname in path.glob('*.csv'):
            name = str(fname).split('.')[-2].split('\\')[-1]
            item = QListWidgetItem(name)
            self.listAssetDb.addItem(item)

    def listAssetDbChanged(self):
        if self.listAssetDb.currentItem() == None:
            self.buttonLoadDb.setEnabled(False)
            self.buttonRemoveDb.setEnabled(False)
        else:
            self.buttonLoadDb.setEnabled(True)
            self.buttonRemoveDb.setEnabled(True)

    def buttonRetrDbClicked(self):
        self.statusBar().showMessage('Retrieving database from server...')
        name = datetime.now().strftime('%Y-%m-%d %H-%M-%S')
        self.portfolio.retrieve_asset_database()
        self.portfolio.save_asset_database(self.data_folder + name)
        item = QListWidgetItem(name)
        self.listAssetDb.addItem(item)
        self.statusBar().showMessage('Database successfully retrieved.')
        self.updateDatabaseTable()

    def buttonRemoveDbClicked(self):
        item = self.listAssetDb.takeItem(self.listAssetDb.currentRow())
        os.remove(self.data_folder + item.text() + '.csv')
        self.statusBar().showMessage('Database "' + item.text() + '" removed')
        item = None

    def buttonLoadDbClicked(self):
        name = self.listAssetDb.currentItem().text()
        self.portfolio.load_asset_database(self.data_folder + name)
        self.updateDatabaseTable()
        print('Database "' + name + '" loaded')
        self.statusBar().showMessage('Database "' + name + '" loaded')

    ###

if __name__ == '__main__':

    app = QApplication(sys.argv)
    ex = App()
    sys.exit(app.exec_())
