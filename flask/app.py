from flask import Flask, jsonify, render_template, request, flash, redirect, session, url_for;
from flask_cors import CORS;
from flask_sqlalchemy import SQLAlchemy;
import requests;
from markupsafe import escape;
from flask_user import login_required, UserManager, UserMixin
from werkzeug.security import check_password_hash, generate_password_hash
from sqlalchemy import event
from sqlalchemy import DDL
from random import seed
from random import random
import uuid
from uuid import uuid1
import xmltodict
import urllib.request as urllib2
from urllib.parse import quote
from pprint import pprint
import json

app = Flask(__name__)
CORS(app)
ENV = 'prod'

# Setting database configs
if ENV == 'dev':
    app.debug = True
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:Biology512@localhost/book_recs'
else:
    app.debug = False
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgres://grrxtpxtklabjt:19192f867330d309d07c38acd0d7f79dc1fef4ccafc757e9e25245d03f54ba20@ec2-52-204-232-46.compute-1.amazonaws.com:5432/d7fl9nj50gmm5f'

app.config['SQL_ALCHEMY_TRACK_MODIFICATIONS'] = False

app.config['SECRET_KEY'] = "OCML3CRawVEueaxcuKHOph"

db = SQLAlchemy(app)

def customid():
    idquery = db.session.query(Ratings).order_by(Ratings.col_id.desc()).first()
    last_id = int(idquery.col_id)
    next_id = int(last_id) + 1
    return next_id

def user_id(userid):
    if db.session.query(Ratings).filter(Ratings.username == userid).count() == 0:
        idquery = db.session.query(Ratings).order_by(Ratings.userid.desc()).first()
        last_id = int(idquery.userid)
        next_id = int(last_id) + 1
        return next_id
    else:
        idquery = db.session.query(Ratings).filter(Ratings.username == userid).first()
        idquery_old = idquery.userid
        return idquery_old

def parse_xml(request_xml):
    # xml_data = request.form['GoodreadsResponse']
    content_dict = xmltodict.parse(request_xml)
    return jsonify(content_dict)

# Building user model
class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(200))
    password = db.Column(db.String(200))
    password_hash = db.Column(db.String(200))

    def __init__(self, username, password, password_hash):
        self.username = username
        self.password = password
        self.password_hash = password_hash


# Building ratings model
class Ratings(db.Model):
    __tablename__ = 'ratings'
    col_id = db.Column(db.Integer, primary_key=True)
    userid = db.Column(db.Integer)
    rating = db.Column(db.Integer)
    book_id = db.Column(db.Integer)
    username = db.Column(db.String(200))
    isbn10 = db.Column(db.String(200))

    def __init__(self, col_id, userid, rating, book_id, username, isbn10):
        self.col_id = col_id
        self.userid = userid
        self.rating = rating
        self.book_id = book_id
        self.username = username
        self.isbn10 = isbn10


# Building routes for the site


# Home page
@app.route('/')
def index():
   return render_template("base.html")

# Registration page
@app.route('/register', methods=["GET", "POST"])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        password_hash = generate_password_hash(password)

        if username == '' or password == '':
            return render_template('register.html', message='Please include a name and/or password.')

        if db.session.query(User).filter(User.username == username).count() == 0:
            user = User(username, password, password_hash)
            db.session.add(user)
            db.session.commit()
            session['username'] = user.username
            session['user_id'] = user_id(session.get('username'))
            return redirect(url_for('search'))
        else:
            return render_template('register.html', message='Sorry, this username is already taken.')

        return render_template('register.html')
    
    else:
        return render_template('register.html')


@app.route('/sign-in', methods=["GET", "POST"])
def sign_in():
    if request.method == 'POST':
        username_entered = request.form['username']
        password_entered = request.form['password']
        user = db.session.query(User).filter(User.username == username_entered).first()
        if user is not None and check_password_hash(user.password_hash, password_entered):
            session['username'] = user.username
            session['user_id'] = user_id(session.get('username'))
            return redirect(url_for('search'))
        return render_template('signin.html', message="Sorry, either your username does not exist or your password does not match.")
    else:
        return render_template('signin.html')

@app.route('/sign-out', methods=["GET", "POST"])
def sign_out():
    if request.method == 'POST':
        session.pop('username', None)
        return redirect(url_for('sign_in'))
    else:
        return render_template('signout.html')


@app.route("/search", methods=['GET', 'POST'])
def search():
    if request.method == 'POST':
        response_string = 'https://www.goodreads.com/search/index.xml?format=xml&key=Ev590L5ibeayXEVKycXbAw&q=' + quote(request.form.get("title"))  
        xml = urllib2.urlopen(response_string)
        data = xml.read()
        xml.close()
        data = xmltodict.parse(data)
        gr_data = json.dumps(data)
        goodreads_fnl = json.loads(gr_data)
        gr = goodreads_fnl['GoodreadsResponse']['search']['results']

    
        if not request.form.get("title"):
            return("Please enter a book title below.")

        return render_template("searchResults.html", books = gr)

    else:
        username = session.get('username')
        return render_template("search.html", username = username)


# book details route
@app.route("/bookDetails/<book_id>")
def bookDetails(book_id):
    response_string = 'https://www.goodreads.com/book/show?id='+ book_id + '&key=Ev590L5ibeayXEVKycXbAw'
    xml = urllib2.urlopen(response_string)
    data = xml.read()
    xml.close()
    data = xmltodict.parse(data)
    gr_data = json.dumps(data)
    goodreads_fnl = json.loads(gr_data)
    gr = goodreads_fnl['GoodreadsResponse']['book']
    return render_template("bookDetails.html", book = gr)



@app.route("/new-rating", methods=['POST'])
def postnew():
    if request.method == 'POST':
        col_id = customid()
        userid = user_id(session.get('username'))
        rating = request.form['rating']
        book_id = request.form.get('bookid')
        username = session.get('username')
        isbn10 = request.form.get('isbn10')

        data = Ratings(col_id, userid, rating, book_id, username, isbn10)
        db.session.add(data)
        db.session.commit()
        return render_template('success.html')

if __name__ == '__main__':
    app.run()